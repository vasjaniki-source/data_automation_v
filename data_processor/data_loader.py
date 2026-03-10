import os
import logging
import pandas as pd
import requests
from io import BytesIO, StringIO
from typing import Any, Optional
from sqlalchemy import create_engine

# Настройка логгера
logger = logging.getLogger(__name__)

class DataLoader:
    """
    Класс для загрузки данных из различных источников: CSV, Excel, SQL, API.
    """

    def __init__(self, config_manager: Any):
        """
        Инициализация загрузчика.
        """
        self.config_manager = config_manager
        self._supported_sources = {'csv', 'excel', 'sql', 'api'}
        logger.info("DataLoader инициализирован.")

    def _check_size(self, file_path: str):
        """Внутренняя проверка размера файла перед загрузкой."""
        # ИСПРАВЛЕНО: Обращение к новой структуре ConfigManager
        file_conf = self.config_manager.get('file_settings', {})
        max_size_mb = file_conf.get('max_file_size_mb', 100)
        
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if file_size_mb > max_size_mb:
            raise ValueError(f"Файл {file_path} превышает лимит: {file_size_mb:.2f}MB > {max_size_mb}MB")

    def _load_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Загрузка данных из CSV с автоматическим определением кодировки."""
        self._check_size(file_path)
        
        encodings = ['utf-8', 'cp1251', 'latin1']
        for encoding in encodings:
            try:
                # kwargs позволяют пользователю передать sep, decimal и т.д.
                df = pd.read_csv(file_path, encoding=encoding, **kwargs)
                logger.info(f"CSV успешно загружен ({encoding}): {file_path}")
                return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        
        raise Exception(f"Не удалось прочитать CSV {file_path}. Проверьте разделители или кодировку.")

    def _load_excel(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Загрузка данных из Excel."""
        self._check_size(file_path)
        
        # openpyxl нужен для .xlsx
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
        """Загрузка данных из БД."""
        try:
            engine = create_engine(conn_string)
            # Используем контекстный менеджер для соединения
            with engine.connect() as conn:
                df = pd.read_sql_query(sql_query, conn, **kwargs)
                logger.info(f"SQL данные загружены. Строк: {len(df)}")
                return df
        except Exception as e:
            logger.error(f"Ошибка SQL: {e}")
            raise

    def _load_api(self, api_url: str, **kwargs) -> pd.DataFrame:
        """Загрузка данных из REST API (JSON, CSV, Excel, TXT, DATA)."""
        try:
            req_params = kwargs.pop('requests_kwargs', {})
            timeout = req_params.pop('timeout', 30)
            
            response = requests.get(api_url, timeout=timeout, **req_params)
            response.raise_for_status()
            
            # Получаем тип контента и приводим к нижнему регистру
            content_type = response.headers.get('Content-Type', '').lower()
            url_lower = api_url.lower()
            
            # 1. Обработка JSON
            if 'json' in content_type:
                return pd.json_normalize(response.json())
            
            # 2. Обработка CSV и текстовых форматов (.data, .txt)
            # Добавляем text/plain и расширения .data, .txt
            elif any(x in content_type for x in ['csv', 'text/plain']) or \
                 url_lower.endswith(('.csv', '.data', '.txt')):
                
                logger.info(f"API ответ определен как текстовый/CSV формат.")
                # Используем StringIO для превращения текста в файлоподобный объект
                return pd.read_csv(StringIO(response.text), **kwargs)
            
            # 3. Обработка Excel
            elif 'excel' in content_type or url_lower.endswith(('.xls', '.xlsx')):
                return pd.read_excel(BytesIO(response.content), **kwargs)
            
            # 4. Последняя попытка: если ничего не подошло, пробуем JSON, потом CSV
            else:
                try:
                    return pd.json_normalize(response.json())
                except:
                    try:
                        return pd.read_csv(StringIO(response.text), **kwargs)
                    except:
                        raise ValueError(f"Не удалось определить формат данных API. Content-Type: {content_type}")
                
        except Exception as e:
            logger.error(f"Ошибка API при обращении к {api_url}: {e}")
            raise

    def load(self, source: str, 
             file_path: Optional[str] = None,
             api_url: Optional[str] = None, 
             sql_query: Optional[str] = None,
             conn_string: Optional[str] = None, 
             **kwargs) -> pd.DataFrame:
        """
        Главный диспетчер загрузки. 
        Маршрутизирует запрос в зависимости от типа источника.
        """
        source = source.lower().strip()

        if source not in self._supported_sources:
            raise ValueError(f"Источник '{source}' не поддерживается. Допустимые: {self._supported_sources}")

        if source == 'csv':
            if not file_path: raise ValueError("Не указан file_path для CSV")
            return self._load_csv(file_path, **kwargs)
            
        elif source == 'excel':
            if not file_path: raise ValueError("Не указан file_path для Excel")
            return self._load_excel(file_path, **kwargs)
            
        elif source == 'sql':
            if not sql_query or not conn_string:
                raise ValueError("Для SQL нужны sql_query и conn_string")
            return self._load_sql(sql_query, conn_string, **kwargs)
            
        elif source == 'api':
            if not api_url: raise ValueError("Не указан api_url для API")
            return self._load_api(api_url, **kwargs)

        raise RuntimeError(f"Непредвиденная ошибка выбора источника: {source}")
