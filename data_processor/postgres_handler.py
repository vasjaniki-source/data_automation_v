
import logging
from typing import Dict, Any, Optional, List
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from psycopg2 import sql
from psycopg2.extras import execute_values
from io import StringIO

logger = logging.getLogger(__name__)


class PostgresHandler:
    """
    Postgres handler with safe SQL formatting and UPSERT support.
    save_dataframe_to_table supports:
      - if_exists: 'append'|'replace'|'fail'
      - conflict_cols: Optional[List[str]] for ON CONFLICT
      - conflict_action: 'update'|'ignore'
      - use_copy: True tries COPY via StringIO, fallback to execute_values
    """

    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        self._connection_params = connection_params.copy() if connection_params else {}
        logger.info("PostgresHandler initialized.")

    @property
    def connection_params(self) -> Dict[str, Any]:
        return self._connection_params

    @connection_params.setter
    def connection_params(self, params: Dict[str, Any]):
        if not isinstance(params, dict):
            raise ValueError("connection_params must be a dict.")
        # Accept either 'database' or 'dbname'
        required = ['host', 'port', 'user', 'database']
        missing = [k for k in required if k not in params or params.get(k) in (None, '')]
        if missing:
            # allow 'dbname' as alias for 'database'
            if 'database' in missing and 'dbname' in params and params.get('dbname'):
                params['database'] = params['dbname']
                missing = [k for k in required if k not in params or params.get(k) in (None, '')]
        if missing:
            raise ValueError(f"Missing connection params: {', '.join(missing)}")
        try:
            params['port'] = int(params['port'])
        except Exception:
            raise ValueError("Parameter 'port' must be int or convertible to int.")
        # Keep copy to avoid side-effects
        self._connection_params = params.copy()
        logger.debug("Connection params set.")

    def _connect(self, override_params: Optional[Dict[str, Any]] = None):
        """
        Возвращает соединение. Если переданы override_params, они берутся за основу (безопасно фильтруются).
        """
        params = self._connection_params.copy() if self._connection_params else {}
        if override_params:
            # берем только допустимые ключи из override_params
            for k in ('host', 'port', 'user', 'password', 'database', 'dbname'):
                if k in override_params and override_params[k] not in (None, ''):
                    params[k] = override_params[k]
        if not params:
            raise ValueError("Connection params not set.")
        try:
            conn = psycopg2.connect(**params)
            return conn
        except OperationalError as e:
            logger.exception("Failed to connect to PostgreSQL: %s", e)
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    @staticmethod
    def _sql_type_for_series(s: pd.Series) -> str:
        dt = str(s.dtype).lower()
        if 'int' in dt:
            return "INTEGER"
        if 'float' in dt or 'double' in dt:
            return "DOUBLE PRECISION"
        if 'bool' in dt:
            return "BOOLEAN"
        if 'datetime' in dt:
            return "TIMESTAMP"
        return "TEXT"

    def _create_table_if_not_exists(self, conn, df: pd.DataFrame, table_name: str, schema: Optional[str] = None):
        if df.empty:
            raise ValueError("Cannot create table from empty DataFrame.")
        columns = []
        for col in df.columns:
            col_name = str(col)
            sql_type = self._sql_type_for_series(df[col])
            columns.append((col_name, sql_type))

        column_defs = sql.SQL(', ').join(
            sql.SQL('{} {}').format(sql.Identifier(name), sql.SQL(dtype))
            for name, dtype in columns
        )

        if schema:
            table_ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table_name))
        else:
            table_ident = sql.Identifier(table_name)

        create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({cols})").format(
            table=table_ident, cols=column_defs
        )

        with conn.cursor() as cur:
            cur.execute(create_query)
            conn.commit()
        logger.info("Ensured table exists: %s%s", f"{schema}." if schema else "", table_name)

    def save_dataframe_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = 'append',
        batch_size: int = 1000,
        use_copy: bool = False,
        conflict_cols: Optional[List[str]] = None,
        conflict_action: str = 'update'
    ) -> int:
        """
        Save DataFrame to table. Supports UPSERT via conflict_cols.
        conflict_action: 'update' -> DO UPDATE SET ..., 'ignore' -> DO NOTHING
        Returns number of rows inserted (approx — for upserts inserted+updated not distinguished).
        """
        if df is None or df.empty:
            raise ValueError("DataFrame is empty.")
        if not table_name:
            raise ValueError("table_name required.")
        if if_exists not in ('append', 'replace', 'fail'):
            raise ValueError("if_exists must be one of 'append', 'replace', 'fail'.")

        conn = self._connect()
        inserted = 0
        try:
            # If replace requested -> drop table first
            if if_exists == 'replace':
                with conn.cursor() as cur:
                    if schema:
                        drop_q = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(schema), sql.Identifier(table_name))
                    else:
                        drop_q = sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name))
                    cur.execute(drop_q)
                    conn.commit()
                    logger.info("Dropped table %s (if existed).", table_name)

            # Ensure table exists (create if necessary)
            self._create_table_if_not_exists(conn, df, table_name, schema=schema)
            columns = [str(c) for c in df.columns]

            # Try COPY first if requested
            if use_copy:
                try:
                    sio = StringIO()
                    # Use tab separator to avoid commas, null as '\N' similar to Postgres default
                    df.to_csv(sio, index=False, header=False, sep='\t', na_rep='\\N')
                    sio.seek(0)
                    column_list = sql.SQL(', ').join(sql.Identifier(c) for c in columns)
                    table_ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table_name)) if schema else sql.Identifier(table_name)
                    copy_sql = sql.SQL("COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')").format(
                        table=table_ident, cols=column_list
                    )
                    with conn.cursor() as cur:
                        cur.copy_expert(copy_sql.as_string(conn), sio)
                        conn.commit()
                        inserted = len(df)
                        logger.info("Inserted %d rows via COPY into %s.", inserted, table_name)
                        return inserted
                except Exception as e:
                    conn.rollback()
                    logger.warning("COPY failed or not possible, falling back to execute_values: %s", e)

            # Build INSERT ... [ON CONFLICT ...] query using psycopg2.sql
            cols_ident = sql.SQL(', ').join(sql.Identifier(c) for c in columns)
            table_ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table_name)) if schema else sql.Identifier(table_name)

            # Prepare conflict clause if needed
            conflict_clause = sql.SQL('')
            if conflict_cols:
                if not all(isinstance(c, str) and c for c in conflict_cols):
                    raise ValueError("conflict_cols must be a list of non-empty strings.")
                conflict_idents = sql.SQL(', ').join(sql.Identifier(c) for c in conflict_cols)
                if conflict_action == 'ignore':
                    conflict_clause = sql.SQL(" ON CONFLICT ({pks}) DO NOTHING").format(pks=conflict_idents)
                elif conflict_action == 'update':
                    non_pk_cols = [c for c in columns if c not in conflict_cols]
                    if non_pk_cols:
                        assignments = sql.SQL(', ').join(
                            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in non_pk_cols
                        )
                        conflict_clause = sql.SQL(" ON CONFLICT ({pks}) DO UPDATE SET {assigns}").format(
                            pks=conflict_idents, assigns=assignments
                        )
                    else:
                        conflict_clause = sql.SQL(" ON CONFLICT ({pks}) DO NOTHING").format(pks=conflict_idents)
                else:
                    raise ValueError("conflict_action must be 'update' or 'ignore'.")

            # Base insert SQL (we keep %s placeholder for execute_values)
            base_insert = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(table=table_ident, cols=cols_ident)
            # Compose full SQL with conflict clause (if any)
            full_insert = sql.Composed([base_insert, conflict_clause])

            # Prepare rows: convert NaN -> None
            rows = [tuple(None if pd.isna(v) else v for v in r) for r in df.itertuples(index=False, name=None)]

            # Insert in batches
            with conn.cursor() as cur:
                for i in range(0, len(rows), batch_size):
                    chunk = rows[i:i + batch_size]
                    # execute_values expects a query string with %s placeholder
                    qstr = full_insert.as_string(conn)
                    execute_values(cur, qstr, chunk, page_size=batch_size)
                    inserted += len(chunk)
                    conn.commit()

            logger.info("Inserted (attempted) %d rows into %s.", inserted, table_name)
            return inserted
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception("Failed to save DataFrame to table %s: %s", table_name, e)
            raise RuntimeError(f"Failed to save data to DB: {e}")
        finally:
            if conn:
                conn.close()

    def load_dataframe_from_table(self, table_name: str, schema: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load data from specified table to pandas.DataFrame.
        Uses psycopg2.sql to safely format identifiers.

        Args:
            table_name: table name (without schema)
            schema: optional schema name
            limit: optional integer limit

        Returns:
            pandas.DataFrame (possibly empty; columns preserved if available)
        """
        if not table_name:
            raise ValueError("Table name must not be empty.")
        if not self._connection_params:
            raise ValueError("Connection params not set. Use setter 'connection_params' first.")

        conn = None
        try:
            conn = self._connect()
            # Build safe identifier
            if schema:
                table_ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table_name))
            else:
                table_ident = sql.Identifier(table_name)

            # Construct SELECT query, possibly with LIMIT
            if limit is not None and isinstance(limit, int) and limit > 0:
                query = sql.SQL("SELECT * FROM {table} LIMIT {lim}").format(table=table_ident, lim=sql.Literal(limit))
            else:
                query = sql.SQL("SELECT * FROM {table}").format(table=table_ident)

            with conn.cursor() as cur:
                cur.execute(query.as_string(conn))
                cols = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()

            if rows:
                df = pd.DataFrame(rows, columns=cols)
            else:
                # If table has known columns, return empty DF with those columns
                df = pd.DataFrame(columns=cols)

            logger.info("Loaded %d rows from %s%s", len(df), f"{schema}." if schema else "", table_name)
            return df
        except Exception as e:
            logger.exception("Error loading DataFrame from DB: %s", e)
            raise RuntimeError(f"Failed to load data from DB: {e}")
        finally:
            if conn:
                conn.close()
                logger.debug("DB connection closed.")
