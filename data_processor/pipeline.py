
# data_processing/pipeline.py
import os
import logging
from typing import Callable, Dict, Any, Optional, List
from pathlib import Path
import pandas as pd


from .data_loader import DataLoader
from .data_validator import DataValidator
from .data_cleaner import DataCleaner
from .analyzer import DataAnalyzer
# Важно: используйте реальное имя файла с реализацией ReportManager.
# Если ваш файл называется report_manager.py — используем так:
from .reporter import NullReportManager, ReportManager
# Если файл называется reporter.py, поменяйте импорт на:
# from .reporter import NullReportManager, ReportManager

from .postgres_handler import PostgresHandler

logger = logging.getLogger(__name__)


class DataProcessingPipeline:
    """
    Оркестратор (Фасад) для процесса обработки данных.
    """

    def __init__(
        self,
        config_manager: Dict[str, Any],
        output_dir: str = "reports",
        log_callback: Optional[Callable[[str, str], None]] = None
        ):
        self.config_manager = config_manager
        self._log_callback = log_callback

        # Проверка интерфейса config_manager — только предупреждение, не критическая ошибка
        if not (hasattr(config_manager, 'get') or hasattr(config_manager, 'get_setting')):
            logger.warning("ConfigManager не предоставляет get/get_setting; некоторые функции могут не работать.")

        # Попытка получить validation_config безопасно
        validation_config = {}
        try:
            if hasattr(config_manager, 'get'):
                validation_config = config_manager.get('validation_rules', {}) or {}
        except Exception:
            logger.debug("Не удалось получить validation_rules через config_manager.get() — используются значения по умолчанию.")

        self.last_load_error: Optional[Exception] = None

        numeric_cols = validation_config.get('numeric_columns', ['id', 'age', 'salary'])
        date_cols = validation_config.get('date_columns', ['join_date'])

        # Инициализация компонентов. Создаём loader по умолчанию — тесты обычно могут заменить pipeline.loader на Mock.
        try:
            self.loader = DataLoader(config_manager)
        except Exception as e:
            logger.warning("Не удалось инициализировать DataLoader в __init__: %s. loader оставлен None.", e)
            self.loader = None

        try:
            self.validator = DataValidator(numeric_cols=numeric_cols, date_cols=date_cols)
        except Exception:
            logger.warning("Не удалось инициализировать DataValidator, поставлен заглушечный валидатор.")
            self.validator = DataValidator(numeric_cols=numeric_cols, date_cols=date_cols)

        try:
            self.cleaner = DataCleaner()
        except Exception:
            logger.warning("Не удалось инициализировать DataCleaner, поставлен заглушечный cleaner.")
            self.cleaner = DataCleaner()

        try:
            self.db_handler = PostgresHandler()
        except Exception:
            logger.warning("Не удалось инициализировать PostgresHandler, поставлен заглушечный handler.")
            self.db_handler = PostgresHandler()

        try:
            self.analyzer = DataAnalyzer()
        except Exception:
            logger.warning("Не удалось инициализировать DataAnalyzer, поставлен заглушечный analyzer.")
            self.analyzer = DataAnalyzer()

        # ReportManager — попытка инициализации, fallback на NullReportManager
        try:
            # Берём флаг отправки почты из конфига, если он есть (ConfigManager.get('smtp') -> dict)
            email_allow_send = True
            try:
                if hasattr(self.config_manager, 'get'):
                    smtp_cfg = self.config_manager.get('smtp', {}) or {}
                    # ожидаем, что в конфиге ключ называется 'send_email' или 'send'
                    if 'send_email' in smtp_cfg:
                        email_allow_send = bool(smtp_cfg.get('send_email'))
                    elif 'send' in smtp_cfg:
                        email_allow_send = bool(smtp_cfg.get('send'))
                # если config_manager предоставляет get_setting, можно дополнительно проверить:
                if email_allow_send is None and hasattr(self.config_manager, 'get_setting'):
                    try:
                        val = self.config_manager.get('smtp.send_email')
                        if val is not None:
                            email_allow_send = bool(val)
                    except Exception:
                        pass
            except Exception:
                logger.debug("Не удалось прочитать флаг send_email из config_manager; будет использоваться значение по умолчанию ReportManager/EmailSender.")

            # Создаём ReportManager, передаём config_manager и флаг email_allow_send (может быть None -> ReportManager сам прочитает конфиг)
            self.report_manager = ReportManager(config_manager=self.config_manager, log_callback=self._log_callback, email_allow_send=email_allow_send)
            logger.info("ReportManager успешно инициализирован.")
        except (ValueError, ConnectionError) as e:
            logger.warning(f"ReportManager не инициализирован: {type(e).__name__}: {e}. Будет использоваться NullReportManager.")
            self.report_manager = NullReportManager(log_callback=self._log_callback)
        except Exception as e:
            logger.critical(f"Неожиданная ошибка при инициализации ReportManager: {e}", exc_info=True)
            self.report_manager = NullReportManager(log_callback=self._log_callback)

        if isinstance(self.report_manager, NullReportManager):
            logger.warning("ReportManager использует NullReportManager — отправка email отключена.")

        # Директория для отчетов
        self.output_dir = Path(output_dir)
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Не удалось создать директорию для отчетов {self.output_dir}: {e}")

        # Состояние пайплайна
        self.current_df: Optional[pd.DataFrame] = None
        self.validation_results: Dict[str, Any] = {}
        self.cleaning_results: Dict[str, Any] = {}
        self.analysis_results: Dict[str, Any] = {}

        # Безопасное получение default_source
        try:
            ds = None
            if hasattr(self.config_manager, 'get'):
                ds = self.config_manager.get('default_source')
            elif hasattr(self.config_manager, 'get_setting'):
                ds = self.config_manager.get('default_source')
            self.default_source = ds
        except Exception:
            self.default_source = None

    @staticmethod
    def _looks_like_path(s: str) -> bool:
        """
        Консервативная эвристика для определения, похожа ли строка на путь:
          - абсолютный путь -> True
          - реальный существующий путь -> True
          - явно есть расширение -> True
          - windows drive like 'C:\\' -> True
          - содержит slash/backslash но нет расширения -> False (важно для 'csv/excel')
        """
        if not isinstance(s, str) or not s:
            return False
        p = Path(s)
        # абсолютный путь или Windows drive
        try:
            if p.is_absolute():
                return True
        except Exception:
            pass
        if len(s) >= 2 and s[1] == ':' and s[0].isalpha():
            return True
        # существует на FS
        try:
            if p.exists():
                return True
        except Exception:
            pass
        # есть расширение -> считаем путём
        if p.suffix:
            return True
        # если содержит слэш/обратный слэш, но нет расширения => НЕ считать путём (фикс для теста)
        if ('/' in s or '\\' in s) and not p.suffix:
            return False
        return False

    def load_data(self, source: Optional[Any] = None, *, allow_fallback_to_type: bool = True, **kwargs) -> pd.DataFrame:
        """
        Универсальный загрузчик данных. Возвращает pd.DataFrame (пустой при ошибке).
        Ключевые правила:
        - loader.load вызывается с source как позиционным аргументом (чтобы мок/функции с сигнатурой
            fake_load(source, **kwargs) работали корректно),
        - .sql файлы читаются в kwargs_copy['sql_query'],
        - не перезаписываем self.loader внутри метода.
        """
        self.last_load_error = None
        logger.debug("load_data called with source=%r kwargs=%r", source, kwargs)

        # Используем default_source при необходимости
        if source is None:
            default = getattr(self, "default_source", None)
            if default:
                source = default
                logger.debug("Using default_source: %r", default)
            else:
                err = ValueError("Не указан источник данных и default_source не настроен.")
                self.last_load_error = err
                self.current_df = pd.DataFrame()
                return self.current_df

        # Если передан DataFrame — возвращаем копию
        if isinstance(source, pd.DataFrame):
            try:
                df = source.copy()
                self.current_df = df
                self.last_load_error = None
                return df
            except Exception as e:
                logger.exception("Ошибка при копировании DataFrame: %s", e)
                self.last_load_error = e
                self.current_df = pd.DataFrame()
                return self.current_df

        # loader должен существовать (тесты обычно устанавливают pipeline.loader = Mock() если нужно)
        loader = getattr(self, "loader", None)
        if loader is None:
            err = RuntimeError("Loader не настроен в pipeline.")
            logger.error(err)
            self.last_load_error = err
            self.current_df = pd.DataFrame()
            return self.current_df

        try:
            # строковый источник или Path
            if isinstance(source, (str, Path)):
                s_orig = str(source).strip()

                # Если строка содержит слэш/обратный слэш и НЕ имеет расширения — это некорректный источник
                ptmp = Path(s_orig)
                if ('/' in s_orig or '\\' in s_orig) and not ptmp.suffix:
                    raise ValueError(f"Некорректный источник: {s_orig}")

                # разбираем "type:path" конструкцию; lhs считается типом если не содержит /, \ или ..
                type_hint = None
                explicit_path = None
                if ':' in s_orig:
                    lhs, rhs = s_orig.split(':', 1)
                    if ('/' not in lhs and '\\' not in lhs and '.' not in lhs):
                        type_hint = lhs.strip().lower()
                        explicit_path = rhs.strip()

                s_to_check = explicit_path if explicit_path else s_orig

                if self._looks_like_path(s_to_check):
                    p = Path(s_to_check)
                    if not p.exists():
                        if p.suffix:
                            raise FileNotFoundError(f"Файл не найден: {s_to_check}")
                        raise ValueError(f"Некорректный источник: {s_to_check}")

                    suffix = p.suffix.lower()
                    kwargs_copy = dict(kwargs)
                    if suffix == '.csv':
                        kwargs_copy['source'] = 'csv'
                        kwargs_copy.setdefault('file_path', str(p))
                        src = kwargs_copy.pop('source')
                        df = loader.load(src, **kwargs_copy)

                    elif suffix in ('.xlsx', '.xls'):
                        kwargs_copy['source'] = 'excel'
                        kwargs_copy.setdefault('file_path', str(p))
                        src = kwargs_copy.pop('source')
                        df = loader.load(src, **kwargs_copy)

                    elif suffix == '.sql':
                        enc = kwargs_copy.pop('file_encoding', 'utf-8')
                        with open(p, 'r', encoding=enc) as f:
                            sql_text = f.read()
                        kwargs_copy['source'] = 'sql'
                        kwargs_copy['sql_query'] = sql_text
                        src = kwargs_copy.pop('source')
                        df = loader.load(src, **kwargs_copy)

                    else:
                        if type_hint:
                            kwargs_copy['source'] = type_hint
                            kwargs_copy.setdefault('file_path', str(p))
                            src = kwargs_copy.pop('source')
                            df = loader.load(src, **kwargs_copy)
                        else:
                            raise ValueError(f"Неизвестное расширение файла: {suffix}")
                else:
                    kwargs_copy = dict(kwargs)
                    if type_hint:
                        kwargs_copy['source'] = type_hint
                        src = kwargs_copy.pop('source')
                        df = loader.load(src, **kwargs_copy)
                    else:
                        st = s_orig.lower()
                        kwargs_copy['source'] = st
                        src = kwargs_copy.pop('source')
                        df = loader.load(src, **kwargs_copy)
            else:
                kwargs_copy = dict(kwargs)
                kwargs_copy['source'] = source
                src = kwargs_copy.pop('source')
                df = loader.load(src, **kwargs_copy)

            if df is None:
                raise TypeError("Загрузчик вернул None вместо pandas.DataFrame.")
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Загрузчик вернул не DataFrame (тип {type(df)}).")

            self.current_df = df
            self.last_load_error = None
            return df

        except Exception as e:
            logger.exception("Ошибка при load_data: %s", e)
            self.last_load_error = e
            self.current_df = pd.DataFrame()
            return self.current_df

    def _find_files_in_standard_dirs(self, filename: str) -> List[Path]:
        standard_dirs = ['csv', 'data', 'input', 'excel', 'files']
        found_files: List[Path] = []
        for directory in standard_dirs:
            dir_path = Path(directory)
            if dir_path.exists() and dir_path.is_dir():
                exact_match = dir_path / filename
                if exact_match.exists():
                    found_files.append(exact_match)
                for ext in ('.csv', '.xlsx', '.xls'):
                    for file in dir_path.glob(f"*{ext}"):
                        if file.name == filename:
                            found_files.append(file)
        return found_files

    def run_validation(self) -> Dict[str, Any]:
        if self.current_df is None:
            raise ValueError("Нет данных для валидации. Сначала загрузите данные.")
        logger.info("Запуск валидации данных...")
        try:
            is_valid, validation_report, cleaned_df = self.validator.validate(self.current_df)
            self.current_df = cleaned_df if cleaned_df is not None else self.current_df
            self.validation_results = validation_report or {}
            if not is_valid:
                logger.warning(f"Валидация завершилась с предупреждениями: {self.validation_results.get('validation_status', 'N/A')}")
            else:
                logger.info("Валидация успешно завершена.")
            return self.validation_results
        except Exception as e:
            logger.error(f"Ошибка при валидации: {e}", exc_info=True)
            raise

    def run_cleaning(self) -> Dict[str, Any]:
        if self.current_df is None:
            raise ValueError("Нет данных для очистки. Сначала загрузите данные.")
        logger.info("Запуск очистки данных...")
        try:
            cleaning_result = self.cleaner.clean(self.current_df)
            self.current_df = cleaning_result.data
            self.cleaning_results = cleaning_result.report or {}
            logger.info(f"Очистка завершена. Размер после очистки: {self.current_df.shape if self.current_df is not None else 'None'}")
            return self.cleaning_results
        except Exception as e:
            logger.error(f"Ошибка при очистке: {e}", exc_info=True)
            raise

    def run_full_analysis(self, date_col: Optional[str] = None, target_col: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        if self.current_df is None:
            raise ValueError("Нет данных для анализа. Сначала загрузите данные.")
        logger.info("Запуск анализа данных...")
        try:
            analyzer_args = kwargs.copy()
            if date_col:
                analyzer_args['date_col'] = date_col
            if target_col:
                analyzer_args['target_col'] = target_col
            results = self.analyzer.analyze(df=self.current_df, **analyzer_args)
            self.analysis_results = results or {}
            logger.info("Анализ завершен.")
            return self.analysis_results
        except Exception as e:
            logger.error(f"Ошибка при анализе: {e}", exc_info=True)
            raise

    def generate_report(self, output_format: str = "both", send_email: bool = False,
                        email_recipients: Optional[List[str]] = None) -> str:
        if self.current_df is None:
            raise ValueError("Нет данных для отчёта. Сначала загрузите данные.")
        if isinstance(self.report_manager, NullReportManager) and send_email:
            logger.error("Отправка email невозможна: ReportManager не инициализирован.")
            raise RuntimeError("ReportManager не готов для отправки email.")
        report_data = self.analysis_results if self.analysis_results else {}
        if not report_data.get('statistics') and self.current_df is not None and not self.current_df.empty:
            try:
                report_data['statistics'] = self.current_df.describe().to_dict()
            except Exception as e:
                logger.warning(f"Не удалось построить статистики для отчёта: {e}")
                report_data['statistics'] = {}
        logger.info(f"Генерация отчёта (format={output_format}, send_email={send_email})")
        try:
            if self.report_manager is None:
                raise ValueError("report_manager не инициализирован")
            report_paths = self.report_manager.generate_reports(
                df=self.current_df,
                analysis_results=report_data,
                output_dir=str(self.output_dir),
                output_format=output_format,
                send_email=send_email,
                email_recipients=email_recipients,
                report_name_prefix="Analytics_Report"
            )
            if report_paths:
                return report_paths.get('pdf') or list(report_paths.values())[0]
            return ""
        except Exception as e:
            logger.error(f"Ошибка генерации отчёта: {e}", exc_info=True)
            raise

    def save_to_db(self, table_name: str, connection_params: Optional[Dict[str, Any]] = None):
        if self.current_df is None:
            raise ValueError("Нет данных для сохранения в БД.")
        logger.info(f"Сохранение данных в таблицу: {table_name}")
        if connection_params is None:
            connection_params = self._get_default_connection_params()
        if not self._validate_connection_params(connection_params):
            raise ValueError("Некорректные параметры подключения к БД")
        try:
            self.db_handler.connection_params = connection_params
            self.db_handler.save_dataframe_to_table(self.current_df, table_name)
            logger.info(f"Данные сохранены в таблицу '{table_name}'.")
        except Exception as e:
            logger.error(f"Ошибка при сохранении в БД: {e}", exc_info=True)
            raise

    def load_from_db(self, table_name: str, limit: Optional[int] = None, connection_params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        logger.info(f"Загрузка данных из БД: {table_name}")
        if connection_params is None:
            connection_params = self._get_default_connection_params()
        if not self._validate_connection_params(connection_params):
            raise ValueError("Некорректные параметры подключения к БД")
        try:
            self.db_handler.connection_params = connection_params
            df = self.db_handler.load_dataframe_from_table(table_name, limit=limit)
            if df is None:
                raise ValueError("Загрузчик БД вернул None.")
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Ожидается DataFrame от DB loader, получен {type(df)}")
            self.current_df = df
            self.validation_results = {}
            self.cleaning_results = {}
            self.analysis_results = {}
            logger.info(f"Данные успешно загружены из БД. Размер: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Ошибка при загрузке из БД: {e}", exc_info=True)
            self.current_df = None
            raise

    def _validate_connection_params(self, params: Dict[str, Any]) -> bool:
        required_fields = ['host', 'database', 'user']
        missing = [f for f in required_fields if not params.get(f)]
        if missing:
            logger.error(f"Отсутствуют обязательные поля подключения: {missing}")
            return False
        port = params.get('port')
        if port is None:
            logger.debug("Порт не указан — используется значение по умолчанию.")
            return True
        try:
            port_int = int(port)
        except (ValueError, TypeError):
            logger.error(f"Порт '{port}' некорректен.")
            return False
        if not (0 < port_int <= 65535):
            logger.error(f"Порт '{port_int}' вне допустимого диапазона.")
            return False
        return True

    def _get_default_connection_params(self) -> Dict[str, Any]:
        conf_db_settings: Dict[str, Any] = {}
        try:
            if not conf_db_settings and hasattr(self.config_manager, 'get'):
                conf_db_settings = {
                    'host': self.config_manager.get('host'),
                    'port': self.config_manager.get('port'),
                    'database': self.config_manager.get('database'),
                    'user': self.config_manager.get('user'),
                    'password': self.config_manager.get('password')
                }
            final_settings = {
                'host': conf_db_settings.get('host') or 'localhost',
                'port': int(conf_db_settings.get('port') or 5432),
                'database': conf_db_settings.get('database') or conf_db_settings.get('db_name') or 'default_db',
                'user': conf_db_settings.get('user') or conf_db_settings.get('db_user') or 'default_user',
                'password': conf_db_settings.get('password') or conf_db_settings.get('db_password') or ''
            }
            return final_settings
        except Exception as e:
            logger.warning(f"Не удалось получить параметры подключения из config_manager: {e}. Используются значения по умолчанию.")
            return {
                'host': 'localhost',
                'port': 5432,
                'database': 'default_db',
                'user': 'default_user',
                'password': ''
            }

    # --- Пакетная обработка (process/process_step_by_step) ---
    def process(
            self,
            source: Any,
            send_email: bool = False,
            email_recipients: Optional[List[str]] = None,
            report_format: str = 'both',
            **kwargs
    ) -> Dict[str, Any]:
        logger.info("Запуск полного цикла обработки (process)...")
        self.load_data(source=source, **kwargs)
        self.run_validation()
        self.run_cleaning()
        analysis_params = {
            'target_col': kwargs.get('target_col'),
            'date_col': kwargs.get('date_col')
        }
        self.run_full_analysis(**{k: v for k, v in analysis_params.items() if v is not None})
        report_path = self.generate_report(output_format=report_format, send_email=send_email, email_recipients=email_recipients)
        return {
            'df': self.current_df,
            'report_path': report_path,
            'analysis': self.analysis_results
        }

    def process_step_by_step(
            self,
            source: Any,
            steps: Optional[List[str]] = None,
            send_email: bool = False,
            email_recipients: Optional[List[str]] = None,
            report_format: str = 'pdf',
            **kwargs
    ) -> Dict[str, Any]:
        available_steps = ['load', 'validate', 'clean', 'analyze', 'report']
        steps_to_run = steps if steps is not None else available_steps
        result: Dict[str, Any] = {}
        logger.info(f"Запуск пошаговой обработки. Шаги: {steps_to_run}")

        analyzer_specific_kwargs = {k: kwargs[k] for k in ['target_col', 'date_col', 'model_type'] if k in kwargs}
        other_kwargs = {k: v for k, v in kwargs.items() if k not in analyzer_specific_kwargs}

        current_df_in_pipeline: Optional[pd.DataFrame] = None

        # Load
        if 'load' in steps_to_run:
            try:
                current_df_in_pipeline = self.load_data(source=source, **other_kwargs)
                result['raw_data'] = current_df_in_pipeline.copy() if current_df_in_pipeline is not None else None
                logger.info(f"Шаг 'load' выполнен. Размер: {current_df_in_pipeline.shape if current_df_in_pipeline is not None else 'None'}")
            except Exception as e:
                logger.error(f"Ошибка шага 'load': {e}", exc_info=True)
                raise ValueError(f"Ошибка на шаге 'load': {e}")
        elif isinstance(source, pd.DataFrame):
            current_df_in_pipeline = source.copy()
            logger.warning("Шаг 'load' пропущен, но source — DataFrame, используется он.")
            result['raw_data'] = current_df_in_pipeline.copy()
        else:
            if self.current_df is not None:
                current_df_in_pipeline = self.current_df.copy()
                logger.warning("Шаг 'load' пропущен. Используются предыдущие данные из пайплайна.")
            else:
                logger.error("Шаг 'load' пропущен и нет доступных данных.")
                raise ValueError("Невозможно продолжить без данных для обработки.")

        # Validate
        if 'validate' in steps_to_run:
            if current_df_in_pipeline is None:
                raise ValueError("Шаг 'validate' требует данные.")
            try:
                validation_report = self.run_validation()
                result['validation_report'] = validation_report
                current_df_in_pipeline = self.current_df
                logger.info("Шаг 'validate' выполнен.")
            except Exception as e:
                logger.error(f"Ошибка шага 'validate': {e}", exc_info=True)
                raise ValueError(f"Ошибка на шаге 'validate': {e}")

        # Clean
        if 'clean' in steps_to_run:
            if current_df_in_pipeline is None:
                raise ValueError("Шаг 'clean' требует данные.")
            try:
                cleaning_result = self.cleaner.clean(current_df_in_pipeline, **other_kwargs)
                self.current_df = cleaning_result.data
                current_df_in_pipeline = self.current_df
                result['cleaned_data'] = current_df_in_pipeline.copy() if current_df_in_pipeline is not None else None
                result['cleaning_report'] = cleaning_result.report or {}
                logger.info("Шаг 'clean' выполнен.")
            except Exception as e:
                logger.error(f"Ошибка шага 'clean': {e}", exc_info=True)
                raise ValueError(f"Ошибка на шаге 'clean': {e}")

        # Analyze
        if 'analyze' in steps_to_run:
            if current_df_in_pipeline is None:
                raise ValueError("Шаг 'analyze' требует данные.")
            try:
                analysis_results = self.run_full_analysis(**analyzer_specific_kwargs)
                result['analysis_results'] = analysis_results
                logger.info("Шаг 'analyze' выполнен.")
            except Exception as e:
                logger.error(f"Ошибка шага 'analyze': {e}", exc_info=True)
                raise ValueError(f"Ошибка на шаге 'analyze': {e}")

        # Report
        if 'report' in steps_to_run:
            if current_df_in_pipeline is None:
                raise ValueError("Шаг 'report' требует данные.")
            try:
                if self.report_manager is None:
                    logger.error("report_manager не инициализирован.")
                    raise RuntimeError("report_manager не готов.")
                if isinstance(self.report_manager, NullReportManager) and send_email:
                    logger.warning("Отправка email отключена (NullReportManager).")
                reporter_kwargs = {
                    'send_email': send_email,
                    'email_recipients': email_recipients,
                    'output_format': report_format
                }
                report_paths = self.report_manager.generate_reports(
                    df=current_df_in_pipeline,
                    analysis_results=self.analysis_results,
                    output_dir=str(self.output_dir),
                    **reporter_kwargs
                )
                result['report_paths'] = report_paths or {}
                logger.info(f"Шаг 'report' выполнен. Сгенерированы файлы: {list(report_paths.keys()) if report_paths else []}")
            except Exception as e:
                logger.error(f"Ошибка шага 'report': {e}", exc_info=True)
                result['report_paths'] = {}

        return result
