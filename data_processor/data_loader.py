
import os
import re
import logging
from pathlib import Path
from typing import Any, Optional, Tuple, List, Set
import pandas as pd
import requests
from io import BytesIO, StringIO
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Класс для загрузки данных из различных источников: CSV, Excel, SQL, API.
    Интерфейс:
      load(source: str, file_path: Optional[str]=None, api_url: Optional[str]=None,
           sql_query: Optional[str]=None, conn_string: Optional[str]=None, **kwargs) -> pd.DataFrame
    Поддерживаемые source: 'csv', 'excel', 'sql', 'api'
    """
    def __init__(self, config_manager: Any):
        self.config_manager = config_manager
        self._supported_sources: Set[str] = {'csv', 'excel', 'sql', 'api'}
        self.file_settings = {}
        try:
            self.file_settings = self.config_manager.get('file_settings', {}) or {}
        except Exception:
            self.file_settings = {}

        try:
            max_file_size_value = self.file_settings.get('max_file_size_mb')
            if max_file_size_value is None:
                self.max_file_size_mb = 100
            else:
                self.max_file_size_mb = int(max_file_size_value)
        except (TypeError, ValueError):
            self.max_file_size_mb = 100

        logger.info("DataLoader инициализирован.")

    def _check_size(self, file_path: str):
        try:
            file_size_bytes = os.path.getsize(file_path)
            file_size_mb = file_size_bytes / (1024 * 1024)
            max_size_mb_int = int(self.max_file_size_mb)
            if file_size_mb > max_size_mb_int:
                raise ValueError(f"Файл {file_path} слишком большой ({file_size_mb:.2f} MB). "
                                 f"Максимум: {max_size_mb_int} MB.")
        except FileNotFoundError:
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        except Exception as e:
            logger.error(f"Ошибка при проверке размера файла {file_path}: {e}")
            raise

    def _load_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        self._check_size(file_path)
        encodings = ['utf-8', 'cp1251', 'latin1']
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, **kwargs)
                logger.info(f"CSV успешно загружен ({encoding}): {file_path}")
                return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        raise Exception(f"Не удалось прочитать CSV {file_path}. Проверьте кодировку/разделители.")

    def _load_excel(self, file_path: str, **kwargs) -> pd.DataFrame:
        self._check_size(file_path)
        params = {'sheet_name': 0, 'engine': 'openpyxl'}
        params.update(kwargs)
        try:
            df = pd.read_excel(file_path, **params)
            logger.info(f"Excel успешно загружен: {file_path}")
            return df
        except Exception as e:
            logger.error(f"Ошибка при чтении Excel {file_path}: {e}")
            raise

    def _load_sql(self, sql_query: str, conn_string: str, **kwargs) -> pd.DataFrame:
        try:
            engine = create_engine(conn_string)
            with engine.connect() as conn:
                df = pd.read_sql_query(sql_query, conn, **kwargs)
                logger.info(f"SQL данные загружены. Строк: {len(df)}")
                return df
        except Exception as e:
            logger.error(f"Ошибка SQL: {e}")
            raise

    def _load_api(self, api_url: str, **kwargs) -> pd.DataFrame:
        try:
            req_params = kwargs.pop('requests_kwargs', {}) or {}
            timeout = req_params.pop('timeout', 30)
            response = requests.get(api_url, timeout=timeout, **req_params)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '').lower()
            url_lower = api_url.lower()

            if 'json' in content_type:
                return pd.json_normalize(response.json())
            elif any(x in content_type for x in ['csv', 'text/plain']) or url_lower.endswith(('.csv', '.data', '.txt')):
                logger.info(f"API ответ определен как текстовый/CSV формат.")
                return pd.read_csv(StringIO(response.text), **kwargs)
            elif 'excel' in content_type or url_lower.endswith(('.xls', '.xlsx')):
                return pd.read_excel(BytesIO(response.content), **kwargs)
            else:
                try:
                    return pd.json_normalize(response.json())
                except Exception:
                    try:
                        return pd.read_csv(StringIO(response.text), **kwargs)
                    except Exception:
                        raise ValueError(f"Не удалось определить формат данных API. Content-Type: {content_type}")
        except Exception as e:
            logger.error(f"Ошибка API при обращении к {api_url}: {e}")
            raise

    # Вспомогательный поиск строки подключения внутри config_manager
    def _get_conn_string_from_config(self) -> Optional[str]:
        try:
            # Популярные ключи
            keys = ['db_connection_string', 'connection_string', 'conn_string', 'conn', 'database_url']
            if hasattr(self.config_manager, 'get'):
                for k in keys:
                    v = self.config_manager.get(k)
                    if v:
                        return v
            if hasattr(self.config_manager, 'get_setting'):
                for k in keys:
                    try:
                        v = self.config_manager.get_setting(k)
                    except Exception:
                        v = None
                    if v:
                        return v
        except Exception:
            pass
        return None

    def load(self, *args, **kwargs) -> pd.DataFrame:
        """
        Гибкий диспетчер загрузки.
        Поддерживает вызовы:
          - load('csv', file_path=...)
          - load(source='sql', sql_query=..., conn_string=...)
          - load(source='sql', file_path='q.sql')  # прочитает файл и вставит sql_query
          - load(source='api', api_url=...)
        Принимает алиасы:
          file_path: path, filepath
          conn_string: conn, connection_string, connection
          sql_query: sql, query
          api_url: url
        """
        # 1) Определяем source: сначала позиционные args, затем kwargs, затем алиасы
        source = None
        if args:
            source = args[0]
        if source is None:
            source = kwargs.pop('source', None)
        if source is None:
            source = kwargs.pop('type', None)
        if source is None:
            source = kwargs.pop('source_type', None)

        if source is None:
            raise ValueError("Не указан параметр 'source' (например 'csv','excel','sql','api').")

        source = str(source).lower().strip()

        # 2) Нормализация аргументов (алиасы)
        file_path = kwargs.pop('file_path', None)
        if not file_path:
            file_path = kwargs.pop('path', None) or kwargs.pop('filepath', None)

        api_url = kwargs.pop('api_url', None)
        if not api_url:
            api_url = kwargs.pop('url', None)

        sql_query = kwargs.pop('sql_query', None)
        if not sql_query:
            sql_query = kwargs.pop('sql', None) or kwargs.pop('query', None)

        conn_string = kwargs.pop('conn_string', None)
        if not conn_string:
            conn_string = kwargs.pop('conn', None) or kwargs.pop('connection_string', None) or kwargs.pop('connection', None)

        # Попытка получить conn_string из конфигурации, если не передан
        if source == 'sql' and not conn_string:
            cs = self._get_conn_string_from_config()
            if cs:
                conn_string = cs

        # 3) Валидация source
        if source not in self._supported_sources:
            raise ValueError(f"Источник '{source}' не поддерживается. Допустимые: {sorted(self._supported_sources)}")

        # 4) Диспатч в зависимости от source
        if source == 'csv':
            if not file_path:
                raise ValueError("Не указан file_path для CSV.")
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Файл не найден: {file_path}")
            return self._load_csv(file_path, **kwargs)

        if source == 'excel':
            if not file_path:
                raise ValueError("Не указан file_path для Excel.")
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Файл не найден: {file_path}")
            return self._load_excel(file_path, **kwargs)

        if source == 'sql':
            # Допустим: sql_query напрямую, либо указали file_path на .sql-файл
            if not sql_query and file_path:
                # если передали путь на .sql файл, прочитаем его
                p = Path(file_path)
                if not p.exists():
                    raise FileNotFoundError(f"SQL файл не найден: {file_path}")
                try:
                    with open(p, 'r', encoding=kwargs.pop('file_encoding', 'utf-8')) as f:
                        sql_query = f.read()
                except Exception as e:
                    raise ValueError(f"Не удалось прочитать SQL файл {file_path}: {e}")

            if not sql_query:
                raise ValueError("Для SQL-источника нужно предоставить 'sql_query' или 'file_path' на .sql файл.")

            if not conn_string:
                raise ValueError("Не указана строка подключения (conn_string) для SQL-запроса.")

            return self._load_sql(sql_query, conn_string, **kwargs)

        if source == 'api':
            if not api_url:
                raise ValueError("Не указан api_url для API.")
            return self._load_api(api_url, **kwargs)

        # защита, хотя условие выше уже проверяет поддерживаемые источники
        raise RuntimeError(f"Непредвиденная ошибка выбора источника: {source}")
