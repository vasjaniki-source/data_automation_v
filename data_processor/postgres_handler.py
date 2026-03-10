import logging
from typing import Dict, Any, Optional
import pandas as pd
import psycopg2
from psycopg2 import OperationalError, Error
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

class PostgresHandler:
    """
    Класс для обработки операций с базой данных PostgreSQL.
    Отвечает за подключение, создание таблиц и сохранение DataFrame.
    """
    
    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        """
        Инициализирует обработчик PostgreSQL.
        Args:
            connection_params: Словарь с параметрами подключения к БД (host, port, user, password, database).
                               Может быть None, если параметры будут установлены позже через setter.
        """
        self._connection_params: Dict[str, Any] = connection_params if connection_params is not None else {}
        logger.info("PostgresHandler инициализирован.")

    @property
    def connection_params(self) -> Dict[str, Any]:
        """Возвращает текущие параметры подключения к БД."""
        return self._connection_params

    @connection_params.setter
    def connection_params(self, params: Dict[str, Any]):
        """Устанавливает параметры подключения к БД."""
        if not all(k in params for k in ['host', 'port', 'user', 'database']):
            raise ValueError("Отсутствуют обязательные параметры подключения (host, port, user, database).")
        
        # Список известных, но потенциально проблемных параметров
        known_bad_params = ["auto_save", "default_table"] 
        
        filtered_params = params.copy()
        for bad_param in known_bad_params:
            if bad_param in filtered_params:
                logger.warning(f"Удален недопустимый параметр подключения '{bad_param}'.")
                del filtered_params[bad_param]
                
        self._connection_params = filtered_params # Сохраняем отфильтрованные параметры
        logger.debug("Параметры подключения к БД обновлены.")

    def _connect(self):
        """Внутренний метод для установления соединения с базой данных."""
        if not self._connection_params:
            raise ValueError("Параметры подключения к БД не установлены.")
        try:
            conn = psycopg2.connect(**self._connection_params)
            logger.debug("Соединение с БД успешно установлено.")
            return conn
        except OperationalError as e:
            logger.error(f"Ошибка подключения к БД: {e}", exc_info=True)
            raise ConnectionError(f"Не удалось подключиться к базе данных: {e}")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при подключении к БД: {e}", exc_info=True)
            raise

    def _get_sql_type(self, pd_type: str) -> str:
        """
        Определяет соответствующий SQL-тип для типа данных Pandas.
        Эта функция может быть расширена для более сложного маппинга.
        """
        if 'int' in pd_type:
            return "INTEGER"
        elif 'float' in pd_type:
            return "REAL"
        elif 'bool' in pd_type:
            return "BOOLEAN"
        elif 'datetime' in pd_type:
            return "TIMESTAMP"
        # Для строковых и прочих объектов, используем TEXT
        return "TEXT"

    def _create_table_if_not_exists(self, conn, df: pd.DataFrame, table_name: str):
        """
        Создает таблицу в базе данных, если она еще не существует,
        на основе схемы DataFrame.
        """
        if df.empty:
            logger.warning(f"DataFrame пуст. Невозможно создать схему для таблицы '{table_name}'.")
            return

        columns_sql = []
        for col, dtype in df.dtypes.items():
            if isinstance(col, str):
                clean_col_name = f'"{col.replace(" ", "_").replace(".", "_").lower()}"'
            else:
                # Преобразуем нестроковое имя в строку
                clean_col_name = f'"{str(col).replace(" ", "_").replace(".", "_").lower()}"'
            sql_type = self._get_sql_type(str(dtype))
            columns_sql.append(f"{clean_col_name} {sql_type}")
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {", ".join(columns_sql)}
        );
        """
        try:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
            logger.info(f"Таблица '{table_name}' проверена/создана в БД.")
        except Error as e:
            conn.rollback()
            logger.error(f"Ошибка при создании/проверке таблицы '{table_name}': {e}", exc_info=True)
            raise

    def save_dataframe_to_table(self, df: pd.DataFrame, table_name: str):
        """
        Сохраняет DataFrame в указанную таблицу PostgreSQL.
        Таблица будет создана, если она не существует.
        
        Args:
            df: DataFrame, который нужно сохранить.
            table_name: Имя таблицы в базе данных.
        """
        if df is None or df.empty:
            raise ValueError("DataFrame для сохранения не может быть пустым.")
        if not table_name:
            raise ValueError("Имя таблицы не может быть пустым.")
        if not self._connection_params:
            raise ValueError("Параметры подключения к БД не установлены. Используйте setter 'connection_params'.")

        conn = None
        try:
            conn = self._connect()
            self._create_table_if_not_exists(conn, df, table_name) # Сначала создаем/проверяем таблицу

            # Подготовка данных для вставки
            # Рекомендуется использовать `execute_values` для эффективной вставки множества строк
            # psycopg2 не любит имена столбцов с пробелами/символами без кавычек, поэтому приводим их к "чистым"
            clean_columns = [f'"{col.replace(" ", "_").replace(".", "_").lower()}"' for col in df.columns]
            
            # SQL-запрос для вставки
            insert_query = f"INSERT INTO \"{table_name}\" ({', '.join(clean_columns)}) VALUES %s"
            
            # Преобразуем DataFrame в список кортежей
            data_to_insert = [tuple(row) for row in df.itertuples(index=False)]

            with conn.cursor() as cur:
                execute_values(cur, insert_query, data_to_insert)
                conn.commit()
            
            logger.info(f"DataFrame успешно сохранен в таблицу '{table_name}'. Вставлено строк: {len(df)}.")

        except Exception as e:
            if conn:
                conn.rollback() # Откатываем транзакцию в случае ошибки
            logger.error(f"Ошибка при сохранении DataFrame в БД: {e}", exc_info=True)
            raise RuntimeError(f"Не удалось сохранить данные в БД: {e}")
        finally:
            if conn:
                conn.close()
                logger.debug("Соединение с БД закрыто.")


    def load_dataframe_from_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Загружает данные из указанной таблицы PostgreSQL в pandas.DataFrame.
        
        Args:
            table_name: Имя таблицы в базе данных.
            limit: Максимальное количество строк для загрузки (необязательно).
        
        Returns:
            Загруженный pandas.DataFrame.
        """
        if not table_name:
            raise ValueError("Имя таблицы не может быть пустым.")
        if not self._connection_params:
            raise ValueError("Параметры подключения к БД не установлены. Используйте setter 'connection_params'.")

        conn = None
        df = pd.DataFrame() # Инициализируем пустой DataFrame
        try:
            conn = self._connect()
            with conn.cursor() as cur:
                # Очищаем имя таблицы от потенциальных проблем
                clean_table_name = table_name.replace('"', '').replace("'", '')
                query = f'SELECT * FROM "{clean_table_name}"'
                if limit is not None and isinstance(limit, int) and limit > 0:
                    query += f' LIMIT {limit}'
                
                cur.execute(query)
                cols = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()
            
            if rows:
                df = pd.DataFrame(rows, columns=cols)
            else:
                logger.info(f"Таблица '{table_name}' пуста или не содержит данных.")
                df = pd.DataFrame(columns=cols) # Возвращаем пустой DataFrame с колонками, если они есть

            logger.info(f"Данные успешно загружены из таблицы '{table_name}'. Загружено строк: {len(df)}.")
            return df

        except Exception as e:
            logger.error(f"Ошибка при загрузке DataFrame из БД: {e}", exc_info=True)
            raise RuntimeError(f"Не удалось загрузить данные из БД: {e}")
        finally:
            if conn:
                conn.close()
                logger.debug("Соединение с БД закрыто.")