from typing import Any, Dict, Optional, List
from pathlib import Path
import logging
import pandas as pd 

# Импорт всех компонентов пайплайна
from .data_loader import DataLoader
from .data_validator import DataValidator
from .data_cleaner import DataCleaner, CleaningResult 
from .analyzer import DataAnalyzer
from .reporter import ReportManager
# Добавляем PostgresHandler, который потребуется для сохранения в БД
from .postgres_handler import PostgresHandler # Убедитесь, что этот файл существует

logger = logging.getLogger(__name__) 

class DataProcessingPipeline:
    """
    Оркестратор (Фасад) для процесса обработки данных.
    Предоставляет высокоуровневый интерфейс для GUI и инкапсулирует
    внутреннюю логику и состояние.
    """
    def __init__( 
        self,
        config_manager: Any, # ConfigManager теперь обязателен для инициализации
        output_dir: str = "reports" # smtp_config теперь будет браться из config_manager
    ):
        self.config_manager = config_manager
        # Убедимся, что config_manager имеет необходимые методы
        if not hasattr(config_manager, 'get_setting') or not hasattr(config_manager, 'get'):
            logger.error("ConfigManager должен иметь методы 'get_setting' и 'get'.")
            raise AttributeError("ConfigManager не соответствует требуемому интерфейсу.")

        # Загружаем правила валидации из конфига
        # Добавлены значения по умолчанию, если ключи отсутствуют
        numeric_cols = config_manager.get_setting('validation_rules.numeric_columns', ['id', 'age', 'salary'])
        date_cols = config_manager.get_setting('validation_rules.date_columns', ['join_date'])
        
        # Инициализация всех компонентов
        self.loader = DataLoader(config_manager) # Предполагается, что DataLoader принимает config_manager
        self.validator = DataValidator(numeric_cols=numeric_cols, date_cols=date_cols)
        self.cleaner = DataCleaner()
        self.db_handler = PostgresHandler() # Инициализация обработчика БД
        self.analyzer = DataAnalyzer()

        # Получаем конфиг SMTP из менеджера настроек. 
        # ReportManager ожидает словарь, который может быть пустым, если настройки не найдены.
        smtp_cfg = self.config_manager.get('smtp', {})
        self.report_manager = ReportManager(smtp_cfg)

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        logger.info(f"Пайплайн обработки данных инициализирован, отчёты будут сохраняться в: {self.output_dir}")

        # --- Внутреннее состояние пайплайна для пошаговой обработки (GUI) ---
        self.current_df: Optional[pd.DataFrame] = None
        self.validation_results: Dict[str, Any] = {}
        self.cleaning_results: Dict[str, Any] = {}
        self.analysis_results: Dict[str, Any] = {}


    # --- Методы для пошагового взаимодействия с GUI ---

    def load_data(self, source: Any, **kwargs) -> pd.DataFrame:
        """
        Загружает данные из указанного источника (путь к файлу, SQL-запрос, DataFrame, API)
        и сохраняет их во внутреннее состояние.
        
        Args:
            source: Тип источника ('csv', 'excel', 'sql', 'api') или DataFrame.
            **kwargs: Параметры для загрузчика (например, file_path, query, api_url).
            
        Returns:
            DataFrame с загруженными данными.
        """
        logger.info(f"Начата загрузка данных из источника: {source}")
        try:
            # Если source - это уже DataFrame
            if isinstance(source, pd.DataFrame):
                df = source.copy() # Используем копию, чтобы не изменять оригинальный DataFrame
                logger.info(f"Данные предоставлены как DataFrame, размер: {df.shape}")
            else:
                # Предполагаем, что source - это тип источника (строка)
                source_type = str(source).lower()
                
                # Если тип 'csv/excel', то file_path должен быть в kwargs
                if source_type == 'csv/excel':
                    file_path = kwargs.get('file_path', '')
                    if not file_path:
                        raise ValueError("Для типа источника 'csv/excel' необходимо указать 'file_path'.")
                    
                    file_path_obj = Path(file_path)
                    if not file_path_obj.is_file():
                        raise FileNotFoundError(f"Указанный файл не найден: {file_path}")

                    # Определяем реальный тип по расширению
                    if file_path_obj.suffix.lower() == '.csv':
                        source_type = 'csv'
                    elif file_path_obj.suffix.lower() in ('.xlsx', '.xls'):
                        source_type = 'excel'
                    else:
                        raise ValueError(f"Неподдерживаемое расширение файла: {file_path_obj.suffix}. Ожидается .csv, .xlsx или .xls")
                
                # Вызываем загрузчик с определенным типом и параметрами
                df = self.loader.load(source=source_type, **kwargs)
                
            # Проверка результата загрузки
            if df is None:
                raise ValueError("Загрузчик вернул None. Проверьте источник данных и параметры.")
            if not isinstance(df, pd.DataFrame):
                 raise TypeError(f"Ожидался DataFrame от загрузчика, но получен {type(df)}.")

            self.current_df = df
            # Сброс результатов предыдущей обработки при загрузке новых данных
            self.validation_results = {}
            self.cleaning_results = {}
            self.analysis_results = {}
            logger.info(f"Данные успешно загружены. Размер: {df.shape}")
            return df
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}", exc_info=True)
            self.current_df = None # Сбрасываем DataFrame при ошибке
            raise 

    def run_validation(self) -> Dict[str, Any]:
        """
        Выполняет валидацию текущего DataFrame.
        
        Returns:
            Словарь с результатами валидации, включая отчет и, возможно, очищенные данные.
        """
        if self.current_df is None:
            raise ValueError("Нет данных для валидации. Сначала загрузите данные.")

        logger.info("Запуск валидации данных...")
        try:
            # Выполняем валидацию. Предполагается, что validate возвращает (is_valid, report, cleaned_df)
            is_valid, validation_report, cleaned_df = self.validator.validate(self.current_df)

            # Обновляем DataFrame, если он был изменен (например, при очистке в процессе валидации)
            self.current_df = cleaned_df if cleaned_df is not None else self.current_df
            self.validation_results = validation_report # Сохраняем отчет
            
            if not is_valid:
                logger.warning(f"Валидация завершена с предупреждениями. Статус: {validation_report.get('validation_status', 'N/A')}")
            else:
                logger.info("Валидация завершена успешно.")
            
            # Возвращаем полный отчет для использования в GUI/тестах
            return self.validation_results
        except Exception as e:
            logger.error(f"Ошибка при валидации данных: {e}", exc_info=True)
            raise

    def run_cleaning(self) -> Dict[str, Any]:
        """
        Выполняет очистку текущего DataFrame.
        Предполагается, что очистка также производится через DataValidator,
        либо через DataCleaner, если он используется отдельно.
        
        Returns:
            Словарь с результатами очистки.
        """
        if self.current_df is None:
            raise ValueError("Нет данных для очистки. Сначала загрузите данные.")
        
        logger.info("Запуск очистки данных...")
        try:               
            # Предполагаем, что DataCleaner.clean возвращает CleaningResult
            # Если DataValidator.validate уже выполняет очистку, этот метод может быть избыточен
            # или должен использовать DataCleaner напрямую.
            # Для примера, используем DataCleaner:
            cleaning_result = self.cleaner.clean(self.current_df)
            self.current_df = cleaning_result.data # Обновляем DataFrame
            self.cleaning_results = cleaning_result.report # Сохраняем отчет
            
            logger.info(f"Очистка данных завершена. Строк после очистки: {self.current_df.shape[0]}")
            return self.cleaning_results
        except Exception as e:
            logger.error(f"Ошибка при очистке данных: {e}", exc_info=True)
            raise

    def run_full_analysis(self, date_col: Optional[str] = None, target_col: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Выполняет полный анализ текущего DataFrame.
        
        Args:
            date_col: Название колонки с датой (для временных рядов).
            target_col: Название целевой колонки (для ML).
            **kwargs: Дополнительные параметры для DataAnalyzer.
            
        Returns:
            Словарь с результатами анализа.
        """
        if self.current_df is None:
            raise ValueError("Нет данных для анализа. Сначала загрузите данные.")
        
        logger.info("Запуск полного анализа данных...")
        try:
            # Формируем аргументы для DataAnalyzer.analyze
            analyzer_args = kwargs.copy()
            if date_col:
                analyzer_args['date_col'] = date_col
            if target_col:
                analyzer_args['target_col'] = target_col
            
            results = self.analyzer.analyze(df=self.current_df, **analyzer_args)
            self.analysis_results = results # Сохраняем результаты
            logger.info("Полный анализ данных завершен.")
            return self.analysis_results
        except Exception as e:
            logger.error(f"Ошибка при анализе данных: {e}", exc_info=True)
            raise

    def generate_report(self, output_format: str = "pdf", send_email: bool = False, 
                        email_recipients: Optional[List[str]] = None) -> str:
        """
        Генерирует отчет на основе текущих данных и результатов анализа.
        
        Args:
            output_format: Формат отчета ('pdf', 'excel', 'both').
            send_email: Отправлять ли отчет по почте.
            email_recipients: Список получателей email.
            
        Returns:
            Путь к первому сгенерированному отчету или пустая строка, если отчеты не были созданы.
        """
        if self.current_df is None:
            raise ValueError("Нет данных для генерации отчета. Сначала загрузите и обработайте данные.")
        
        # Проверяем, был ли запущен анализ, и используем его результаты, если они есть
        analysis_data_for_report = self.analysis_results if self.analysis_results else {}
        if not self.analysis_results and 'analyze' in self.config_manager.get('pipeline_steps', []):
            logger.warning("Результаты анализа отсутствуют. Отчет будет сгенерирован без них.")

        logger.info(f"Генерация отчета в формате: {output_format}")
        try:
            # ReportManager.generate_reports возвращает Dict[str, Path]
            report_paths_dict = self.report_manager.generate_reports(
                df=self.current_df,
                analysis_results=analysis_data_for_report,
                output_dir=str(self.output_dir),
                output_format=output_format, 
                send_email=send_email,
                email_recipients=email_recipients
            )
            # Возвращаем путь к первому сгенерированному отчету
            if report_paths_dict:
                # Получаем ключ первого элемента (например, 'pdf' или 'excel')
                first_report_key = list(report_paths_dict.keys())[0]
                report_path = str(report_paths_dict[first_report_key])
                logger.info(f"Отчет успешно сгенерирован: {report_path}")
                return report_path
            else:
                logger.warning("Генератор отчетов не вернул путь к созданному файлу.")
                return ""
        except Exception as e:
            logger.error(f"Ошибка при генерации отчета: {e}", exc_info=True)
            raise

    def save_to_db(
        self,
        table_name: str,
        connection_params: Optional[Dict[str, Any]] = None
    ):
        """
        Сохраняет текущие обработанные данные в базу данных PostgreSQL.
        
        Args:
            table_name: Название таблицы для сохранения.
            connection_params: Параметры подключения к БД (опционально). Если None, используются параметры по умолчанию из config_manager.
        """
        if self.current_df is None:
            raise ValueError("Нет данных для сохранения в БД. Сначала загрузите и обработайте данные.")

        logger.info(f"Начато сохранение данных в таблицу '{table_name}'...")

        # Если параметры не переданы, используем дефолтные из config_manager
        if connection_params is None:
            connection_params = self._get_default_connection_params()

        # Валидация параметров подключения
        if not self._validate_connection_params(connection_params):
            raise ValueError("Некорректные параметры подключения к БД")

        try:
            # Передаем параметры в db_handler
            self.db_handler.connection_params = connection_params
            self.db_handler.save_dataframe_to_table(self.current_df, table_name)
            logger.info(f"Данные успешно сохранены в таблицу '{table_name}'.")
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в БД: {e}", exc_info=True)
            raise

    # --- НОВЫЙ МЕТОД ДЛЯ ЗАГРУЗКИ ИЗ БД ---
    def load_from_db(
        self,
        table_name: str,
        limit: Optional[int] = None,
        connection_params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Загружает данные из базы данных PostgreSQL и устанавливает их как текущий DataFrame.
        
        Args:
            table_name: Название таблицы для загрузки.
            limit: Максимальное количество строк для загрузки (опционально).
            connection_params: Параметры подключения к БД (опционально). Если None, используются параметры по умолчанию.
        
        Returns:
            DataFrame с загруженными данными.
        """
        logger.info(f"Начата загрузка данных из таблицы '{table_name}' в pipeline...")

        # Если параметры не переданы, используем дефолтные
        if connection_params is None:
            connection_params = self._get_default_connection_params()

        # Валидация параметров подключения
        if not self._validate_connection_params(connection_params):
            raise ValueError("Некорректные параметры подключения к БД")

        try:
            self.db_handler.connection_params = connection_params
            df = self.db_handler.load_dataframe_from_table(table_name, limit=limit)
            
            if df is None:
                raise ValueError("Загрузчик БД вернул None. Проверьте источник данных и параметры.")
            if not isinstance(df, pd.DataFrame):
                 raise TypeError(f"Ожидался DataFrame от загрузчика, но получен {type(df)}.")

            # Обновляем состояние пайплайна
            self.current_df = df
            self.validation_results = {}
            self.cleaning_results = {}
            self.analysis_results = {}
            logger.info(f"Данные успешно загружены из БД. Размер: {df.shape}")
            return df
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из БД: {e}", exc_info=True)
            self.current_df = None # Сбрасываем DataFrame при ошибке
            raise
    # --- КОНЕЦ НОВОГО МЕТОДА ---


    def _validate_connection_params(self, params: Dict[str, Any]) -> bool:
        """Проверяет корректность параметров подключения к БД."""
        required_fields = ['host', 'database', 'user'] # user также обязателен
        missing = [field for field in required_fields if not params.get(field)]
        if missing:
            logger.error(f"Отсутствуют обязательные поля подключения к БД: {', '.join(missing)}")
            return False

        # Проверка порта
        port = params.get('port')
        if not isinstance(port, int) or not (0 < port <= 65535):
            logger.error(f"Порт '{port}' некорректен. Должен быть целым числом от 1 до 65535.")
            return False
            
        return True

    def _get_default_connection_params(self) -> Dict[str, Any]:
        """
        Возвращает параметры подключения к БД по умолчанию из конфигурации.
        Ищет настройки в различных возможных ключах config_manager.
        """
        conf_db_settings = {}
        try:
            # Попытка получить полную секцию 'database'
            if hasattr(self.config_manager, 'get'):
                conf_db_settings = self.config_manager.get('database', {})
            
            # Если секция 'database' пуста или отсутствует, пробуем другие известные ключи
            if not conf_db_settings:
                 # Если есть get_setting, пробуем его
                 if hasattr(self.config_manager, 'get_setting'):
                    conf_db_settings = {
                        'host': self.config_manager.get_setting('db_host', 'localhost'),
                        'port': self.config_manager.get_setting('db_port', 5432),
                        'database': self.config_manager.get_setting('db_name', 'default_db'),
                        'user': self.config_manager.get_setting('db_user', 'default_user'),
                        'password': self.config_manager.get_setting('db_password', '')
                    }
                 # Если нет get_setting, но есть get, пробуем напрямую
                 elif hasattr(self.config_manager, 'get'):
                    conf_db_settings = {
                        'host': self.config_manager.get('host', 'localhost'),
                        'port': self.config_manager.get('port', 5432),
                        'database': self.config_manager.get('database', 'default_db'), # Здесь будет перезаписано, если есть 'db_name'
                        'user': self.config_manager.get('user', 'default_user'),
                        'password': self.config_manager.get('password', '')
                    }
            
            # Комбинируем найденные настройки с жестко заданными значениями по умолчанию
            # Приоритет у настроек из конфига
            return {
                'host': conf_db_settings.get('host', 'localhost'),
                'port': int(conf_db_settings.get('port', 5432)), # Убедимся, что порт int
                'database': conf_db_settings.get('database', 'default_db'),
                'user': conf_db_settings.get('user', 'default_user'),
                'password': conf_db_settings.get('password', '')
            }
        except Exception as e:
            logger.warning(f"Не удалось извлечь настройки подключения к БД из config_manager: {e}. Используются значения по умолчанию.")
            return {
                'host': 'localhost',
                'port': 5432,
                'database': 'default_db',
                'user': 'default_user',
                'password': ''
            }


    # --- Пакетная обработка (process, process_step_by_step) ---
    # Методы, которые выполняют полный цикл обработки данных.

    def process(
            self,
            source: Any, # Может быть pd.DataFrame, путем к файлу, SQL-запросом, API-URL и т.д.
            send_email: bool = False,
            email_recipients: Optional[List[str]] = None,
            report_format: str = 'pdf', # Добавлен параметр формата отчета
            **kwargs
        ) -> Dict[str, Any]:
        """
        Запускает полный цикл обработки данных: загрузка, валидация, очистка,
        анализ и генерация отчётов.
        
        Args:
            source: Источник данных (DataFrame, путь к файлу, SQL-запрос и т.д.).
            send_email: Флаг отправки отчета по email.
            email_recipients: Список email получателей.
            report_format: Формат отчета ('pdf', 'excel', 'both').
            **kwargs: Дополнительные параметры для загрузки, валидации, очистки, анализа.
            
        Returns:
            Словарь с результатами обработки: cleaned_data_df, cleaning_report, analysis_results, report_paths, validation_report.
        """
        logger.info("Запуск полного цикла обработки pipeline")

        # Извлечение специфичных kwargs для Analyzer и Reporter
        analyzer_kwargs = {k: kwargs[k] for k in ['target_col', 'date_col', 'model_type'] if k in kwargs}
        # Убираем эти параметры из общего kwargs, чтобы они не передавались в DataLoader/DataCleaner
        for k in list(analyzer_kwargs.keys()):
            kwargs.pop(k, None) 
            
        # Подготовка reporter_kwargs
        reporter_kwargs = {
            'send_email': send_email,
            'email_recipients': email_recipients,
            'output_format': report_format
        }

        # 1. Загрузка данных
        raw_data = None
        try:
            raw_data = self.load_data(source=source, **kwargs) # Используем метод load_data
            # Если load_data вернул None или ошибку, он поднимет исключение
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных в process: {e}", exc_info=True)
            raise ValueError(f"Не удалось загрузить данные: {e}")

        # 2. Валидация
        validation_report = None
        try:
            validation_report = self.run_validation() # Используем метод run_validation
            # Если валидация не прошла, run_validation должен поднять исключение
            logger.info("Данные успешно прошли валидацию")
        except ValueError as e: # Перехватываем ошибки валидации
            logger.error(f"Ошибка валидации в process: {e}", exc_info=True)
            raise e # Передаем ошибку дальше
        except Exception as e: # Перехватываем другие ошибки
            logger.error(f"Неожиданная ошибка при валидации в process: {e}", exc_info=True)
            raise ValueError(f"Ошибка валидации: {e}")

        # 3. Очистка данных
        cleaning_result_report = None
        try:
            # Важно: run_cleaning обновляет self.current_df
            cleaning_report_data = self.run_cleaning() 
            cleaning_result_report = cleaning_report_data # Сохраняем отчет очистки
            logger.info("Очистка данных завершена")
        except Exception as e:
            logger.error(f"Ошибка при очистке данных в process: {e}", exc_info=True)
            raise ValueError(f"Ошибка очистки: {e}")

        if self.current_df is None: # Проверка после очистки
            raise ValueError("Очистка данных не вернула DataFrame или произошла ошибка.")

        # 4. Анализ данных
        analysis_results = None
        try:
            # Используем run_full_analysis, который сохраняет результаты в self.analysis_results
            analysis_results = self.run_full_analysis(**analyzer_kwargs) 
            logger.info("Анализ данных завершён")
        except Exception as e:
            logger.error(f"Ошибка при анализе данных в process: {e}", exc_info=True)
            raise ValueError(f"Ошибка анализа: {e}")

        # 5. Генерация отчётов
        report_paths_dict = {}
        try:
            # Используем generate_report, который использует self.current_df и self.analysis_results
            # Он вернет путь к первому сгенерированному отчету
            # Для process мы хотим вернуть все пути, поэтому напрямую вызываем report_manager
            report_paths_dict = self.report_manager.generate_reports(
                df=self.current_df,
                analysis_results=self.analysis_results, # Используем сохраненные результаты
                output_dir=str(self.output_dir),
                **reporter_kwargs
            )
            logger.info(f"Отчёты сгенерированы: {list(report_paths_dict.keys())}")
        except Exception as e:
            logger.error(f"Ошибка при генерации отчетов в process: {e}", exc_info=True)
            # Продолжаем, но логируем ошибку
            
        return {
            'cleaned_data_df': self.current_df.copy() if self.current_df is not None else None, 
            'cleaning_report': cleaning_result_report,
            'analysis_results': self.analysis_results,
            'report_paths': report_paths_dict,
            'validation_report': validation_report 
        }

    def process_step_by_step(
            self,
            source: Any, # Может быть pd.DataFrame, путем к файлу, SQL-запросом и т.д.
            steps: Optional[List[str]] = None, 
            send_email: bool = False,
            email_recipients: Optional[List[str]] = None,
            report_format: str = 'pdf', # Добавлен параметр формата отчета
            **kwargs
        ) -> Dict[str, Any]:
        """
        Запускает обработку данных пошагово, выполняя только указанные шаги.
        
        Args:
            source: Источник данных (DataFrame, путь к файлу, SQL-запрос и т.д.).
            steps: Список шагов для выполнения (например, ['load', 'validate']). Если None, выполняются все доступные шаги.
            send_email: Флаг отправки отчета по email.
            email_recipients: Список email получателей.
            report_format: Формат отчета ('pdf', 'excel', 'both').
            **kwargs: Дополнительные параметры для загрузки, валидации, очистки, анализа.
            
        Returns:
            Словарь с результатами выполненных шагов.
        """
        available_steps = ['load', 'validate', 'clean', 'analyze', 'report']
        steps = steps if steps is not None else available_steps 
        result: Dict[str, Any] = {}

        logger.info(f"Запуск пошаговой обработки данных. Шаги: {steps}")

        # Извлечение специфичных kwargs для Analyzer и Reporter
        analyzer_kwargs = {k: kwargs[k] for k in ['target_col', 'date_col', 'model_type'] if k in kwargs}
        # Все остальные kwargs передаются в loader и cleaner
        other_kwargs = {k: v for k, v in kwargs.items() if k not in analyzer_kwargs}
            
        current_df_in_pipeline: Optional[pd.DataFrame] = None 

        # 1. Загрузка данных (load)
        if 'load' in steps:
            try:
                # Используем метод load_data
                current_df_in_pipeline = self.load_data(source=source, **other_kwargs) 
                result['raw_data'] = current_df_in_pipeline.copy() # Сохраняем копию загруженных данных
                logger.info(f"Шаг 'load' выполнен. Размер: {current_df_in_pipeline.shape}")
            except Exception as e:
                logger.error(f"Ошибка при выполнении шага 'load': {e}", exc_info=True)
                raise ValueError(f"Ошибка на шаге 'load': {e}")
        elif isinstance(source, pd.DataFrame): # Если 'load' пропущен, но source - DataFrame
            current_df_in_pipeline = source.copy()
            logger.warning("Шаг 'load' пропущен, но исходные данные (DataFrame) используются.")
            result['raw_data'] = current_df_in_pipeline.copy()
        else:
            # Если 'load' пропущен и source не DataFrame, ищем данные в current_df, если он был установлен ранее
            if self.current_df is not None:
                current_df_in_pipeline = self.current_df.copy()
                logger.warning("Шаг 'load' пропущен. Используются предыдущие загруженные данные.")
            else:
                logger.error("Шаг 'load' пропущен, и нет доступных данных. Невозможно продолжить.")
                raise ValueError("Невозможно выполнить дальнейшие шаги без загруженных данных.")
        
        # 2. Валидация (validate)
        validation_report = None
        if 'validate' in steps:
            if current_df_in_pipeline is None: 
                raise ValueError("Шаг 'validate' требует данные. Загрузка данных не была выполнена или прошла с ошибкой.")

            try:
                validation_report = self.run_validation() # Используем метод run_validation
                result['validation_report'] = validation_report
                # run_validation обновляет self.current_df, если валидация включает очистку
                current_df_in_pipeline = self.current_df # Обновляем DataFrame для следующих шагов
                logger.info("Шаг 'validate' выполнен.")
            except Exception as e:
                logger.error(f"Ошибка при выполнении шага 'validate': {e}", exc_info=True)
                raise ValueError(f"Ошибка на шаге 'validate': {e}")

        # 3. Очистка данных (clean)
        cleaning_result_report = None
        if 'clean' in steps:
            if current_df_in_pipeline is None:
                raise ValueError("Шаг 'clean' требует данные. Предыдущие шаги не были выполнены.")
            try:
                # Используем run_cleaning, который обновляет self.current_df
                cleaning_result_report = self.run_cleaning()
                current_df_in_pipeline = self.current_df  # Обновляем DataFrame

                # Безопасное копирование: проверяем, что DataFrame не None
                if current_df_in_pipeline is not None:
                    result['cleaned_data'] = current_df_in_pipeline.copy()
                    result['cleaning_report'] = cleaning_result_report
                    logger.info("Шаг 'clean' выполнен.")
                else:
                    # Если после очистки DataFrame стал None — сообщаем об этом
                    logger.warning("После выполнения очистки DataFrame стал None. Пропускаем сохранение.")
                    result['cleaned_data'] = None
                    result['cleaning_report'] = cleaning_result_report
            except Exception as e:
                logger.error(f"Ошибка при выполнении шага 'clean': {e}", exc_info=True)
                raise ValueError(f"Ошибка на шаге 'clean': {e}")

        # 4. Анализ данных (analyze)
        analysis_results = None
        if 'analyze' in steps:
            if current_df_in_pipeline is None:
                raise ValueError("Шаг 'analyze' требует данные.")
            
            try:
                # Используем run_full_analysis (или можно создать отдельный analyze_step)
                # run_full_analysis обновляет self.analysis_results
                analysis_results = self.run_full_analysis(**analyzer_kwargs) 
                result['analysis_results'] = analysis_results
                logger.info("Шаг 'analyze' выполнен.")
            except Exception as e:
                logger.error(f"Ошибка при выполнении шага 'analyze': {e}", exc_info=True)
                raise ValueError(f"Ошибка на шаге 'analyze': {e}")

        # 5. Генерация отчётов (report)
        report_paths_dict = {}
        if 'report' in steps:
            if current_df_in_pipeline is None:
                raise ValueError("Шаг 'report' требует данные.")

            # Подготовка reporter_kwargs
            reporter_kwargs = {
                'send_email': send_email,
                'email_recipients': email_recipients,
                'output_format': report_format # Передаем формат отчета
            }

            try:
                # Используем report_manager напрямую для получения всех путей
                report_paths_dict = self.report_manager.generate_reports(
                    df=current_df_in_pipeline,
                    analysis_results=self.analysis_results, # Используем сохраненные результаты
                    output_dir=str(self.output_dir),
                    **reporter_kwargs
                )
                result['report_paths'] = report_paths_dict
                logger.info(f"Шаг 'report' выполнен. Отчёты сгенерированы: {list(report_paths_dict.keys())}")
            except Exception as e:
                logger.error(f"Ошибка при выполнении шага 'report': {e}", exc_info=True)
                # Продолжаем, но логируем ошибку
                result['report_paths'] = {} # Убедимся, что ключ существует
        
        # Убедимся, что все связанные состояния пайплайна обновлены
        self.current_df = current_df_in_pipeline 
        self.validation_results = validation_report if validation_report is not None else self.validation_results
        self.cleaning_results = cleaning_result_report if cleaning_result_report is not None else self.cleaning_results
        self.analysis_results = analysis_results if analysis_results is not None else self.analysis_results

        return result
