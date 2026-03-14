import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import logging
from typing import Dict, Optional, Callable, Any

import pandas as pd
from unittest.mock import Mock  # Для обработки Mock-объекта pipeline

logger = logging.getLogger(__name__)


class DBConfigWidget(ttk.Frame):
    def __init__(self, parent: tk.Widget, pipeline: Any, add_to_history: Optional[Callable[[str, str], None]] = None):
        """
        parent: родительский tkinter виджет
        pipeline: экземпляр DataProcessingPipeline или Mock (в тестах)
        add_to_history: функция вида (message: str, level: str) -> None, может быть None
        """
        super().__init__(parent, padding="20")
        self.pipeline = pipeline
        self.add_to_history = add_to_history
        self.entries: Dict[str, ttk.Entry] = {}
        self.data_summary_label: Optional[ttk.Label] = None
        self.data_preview_text: Optional[scrolledtext.ScrolledText] = None
        self.create_widgets()

    # Вспомогательные методы
    def _log_history(self, message: str, level: str = "INFO"):
        """Безопасный вызов add_to_history (если передан), иначе обычный лог."""
        try:
            if callable(self.add_to_history):
                self.add_to_history(message, level.upper())
            else:
                # fallback в логер
                if level.upper() == "ERROR":
                    logger.error(message)
                elif level.upper() == "WARNING":
                    logger.warning(message)
                else:
                    logger.info(message)
        except Exception as e:
            logger.exception(f"Ошибка при записи в историю: {e}")

    def _safe_get_config(self, key: str, default: Any = None) -> Any:
        """Безопасно получает значение из pipeline.config_manager (если он есть и не является Mock)."""
        try:
            if self.pipeline is None:
                return default
            # Если pipeline сам Mock — не пытаемся извлечь реальные данные
            if isinstance(self.pipeline, Mock):
                return default
            cm = getattr(self.pipeline, "config_manager", None)
            if cm is None:
                return default
            # Если config_manager похож на dict-like с get
            get = getattr(cm, "get", None)
            if callable(get):
                # не передаём default в get_setting, чтобы учитывать его возможную сигнатуру
                return get(key, default) if "get" in dir(cm) else default
            # Попробуем вызвать get_setting, если есть
            gs = getattr(cm, "get_setting", None)
            if callable(gs):
                res = gs(key)
                return res if res is not None else default
        except Exception as e:
            logger.exception(f"Ошибка при чтении конфигурации: {e}")
        return default

    def create_widgets(self):
        ttk.Label(self, text="Настройка подключения PostgreSQL", font=('Arial', 12, 'bold')).grid(row=0, column=0, columnspan=2, pady=10)

        fields = [
            ("Хост:", "host"), ("Порт:", "port"),
            ("Пользователь:", "user"), ("Пароль:", "password"),
            ("База данных:", "database"),
            ("Имя таблицы:", "table_name")
        ]

        for i, (label, key) in enumerate(fields, 1):
            ttk.Label(self, text=label).grid(row=i, column=0, sticky="w", pady=5)
            show = "*" if key == "password" else ""
            ent = ttk.Entry(self, show=show, width=30)
            ent.grid(row=i, column=1, sticky="ew", padx=5)
            self.entries[key] = ent

        # Безопасное чтение конфигурации (избегаем обращения к Mock-объектам)
        conf_db = self._safe_get_config('database', {}) or {}
        conf_table_name = self._safe_get_config('db.table_name', 'app_table') or 'app_table'

        # Если conf_db оказался не dict-like, приводим к пустому словарю
        if not isinstance(conf_db, dict):
            conf_db = {}

        for key, entry in self.entries.items():
            if key == 'table_name':
                entry.delete(0, tk.END)
                entry.insert(0, str(conf_table_name))
            else:
                entry.delete(0, tk.END)
                entry.insert(0, str(conf_db.get(key, "")))

        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=len(fields) + 1, column=0, columnspan=2, pady=20)

        ttk.Button(btn_frame, text="Проверить подключение", command=self.test_connection).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Сохранить конфиг", command=self.save_config).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Сохранить в БД", command=self.on_save_to_db).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Загрузить из БД", command=self.on_load_from_db).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Отправить на анализ", command=self.on_send_for_analysis).pack(side="left", padx=5)

        data_display_frame = ttk.LabelFrame(self, text="Загруженные данные", padding="10")
        data_display_frame.grid(row=len(fields) + 2, column=0, columnspan=2, sticky="ew", pady=10)

        self.data_summary_label = ttk.Label(data_display_frame, text="Нет данных")
        self.data_summary_label.pack(fill="x", pady=5)

        self.data_preview_text = scrolledtext.ScrolledText(data_display_frame, height=10, width=50, state="disabled", wrap="word", font=("Consolas", 9))
        self.data_preview_text.pack(fill="both", expand=True)

        # Начальный текст в preview
        self.data_preview_text.config(state="normal")
        self.data_preview_text.delete(1.0, tk.END)
        self.data_preview_text.insert(tk.END, "Здесь будет предпросмотр данных после загрузки из БД.")
        self.data_preview_text.config(state="disabled")

        # Растягивание колонок
        self.grid_columnconfigure(1, weight=1)
        data_display_frame.grid_columnconfigure(0, weight=1)

    def get_params(self) -> Dict[str, str]:
        """Возвращает текущие параметры подключения и имя таблицы."""
        result: Dict[str, str] = {}
        for k, v in self.entries.items():
            try:
                val = v.get().strip()
            except Exception:
                val = ""
            result[k] = val
        return result

    def test_connection(self):
        params = self.get_params()

        # Если pipeline — Mock, не пытаемся подключаться, но логируем
        if isinstance(self.pipeline, Mock):
            messagebox.showwarning("Внимание", "Пайплайн не инициализирован. Проверка подключения недоступна.")
            self._log_history("Пайплайн не инициализирован. Проверка подключения недоступна.", "WARNING")
            return

        # Валидация порта (пустая строка -> ошибка, показываем пользователю)
        try:
            port_val = int(params.get('port', '') or 0)
            if not (0 < port_val <= 65535):
                raise ValueError("Порт должен быть числом от 1 до 65535.")
        except ValueError as e:
            messagebox.showerror("Ошибка", f"Некорректный порт: {e}")
            self._log_history(f"Ошибка: некорректный порт '{params.get('port', '')}'", "ERROR")
            return

        # Валидация параметров (если pipeline предоставляет метод _validate_connection_params)
        try:
            if hasattr(self.pipeline, "_validate_connection_params") and callable(getattr(self.pipeline, "_validate_connection_params")):
                valid = self.pipeline._validate_connection_params(params)
                if not valid:
                    messagebox.showerror("Ошибка", "Неполные или некорректные параметры подключения.")
                    self._log_history("Ошибка: неполные параметры подключения к БД.", "ERROR")
                    return
            else:
                # Если нет валидации в pipeline, просто логируем и продолжаем попытку
                logger.debug("Pipeline не предоставляет _validate_connection_params, пропускаем предварительную валидацию.")
        except Exception as e:
            logger.exception(f"Ошибка при валидации параметров подключения: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при проверке параметров: {e}")
            self._log_history(f"Ошибка при валидации параметров подключения: {e}", "ERROR")
            return

        # Попытка тестового подключения через db_handler, если он есть
        try:
            dbh = getattr(self.pipeline, "db_handler", None)
            if dbh is None:
                raise RuntimeError("db_handler не настроен в pipeline.")

            # Собираем только допустимые параметры подключения (без table_name и т.п.)
            db_params = {k: params[k] for k in ['host', 'port', 'user', 'password', 'database'] if params.get(k)}

            # Если db_handler умеет test_connection — вызываем его (передаём параметры)
            if hasattr(dbh, "test_connection") and callable(getattr(dbh, "test_connection")):
                ok = dbh.test_connection(db_params)
                if ok:
                    messagebox.showinfo("Успех", "Подключение установлено!")
                    self._log_history("Успешная проверка подключения к БД", "INFO")
                else:
                    raise ConnectionError("Тест подключения вернул неуспех.")
            else:
                # Пытаемся установить connection_params у handler и вызвать _connect() без аргументов
                if hasattr(dbh, "connection_params"):
                    try:
                        dbh.connection_params = db_params
                    except Exception as e:
                        # Если setter ругается — пробуем вызвать _connect с очищенными params (фоллбек)
                        logger.debug("Не удалось установить dbh.connection_params: %s", e)
                        # не передаём лишние ключи в connect

                # Теперь вызываем _connect() без аргументов, как ожидает PostgresHandler._connect
                if hasattr(dbh, "_connect") and callable(getattr(dbh, "_connect")):
                    conn = dbh._connect()  # НЕ передаём params!
                    try:
                        if hasattr(conn, "close"):
                            conn.close()
                        messagebox.showinfo("Успех", "Подключение установлено!")
                        self._log_history("Успешная проверка подключения к БД", "INFO")
                    except Exception:
                        raise ConnectionError("Соединение установлено, но возникла проблема при закрытии соединения.")
                else:
                    raise RuntimeError("db_handler не предоставляет ни test_connection, ни _connect для проверки.")
        except Exception as e:
            logger.exception(f"Ошибка при проверке подключения: {e}")
            messagebox.showerror("Ошибка подключения", str(e))
            self._log_history(f"Ошибка проверки подключения к БД: {e}", "ERROR")

    def save_config(self):
        params = self.get_params()

        if isinstance(self.pipeline, Mock) or not hasattr(self.pipeline, 'config_manager'):
            messagebox.showwarning("Внимание", "Менеджер конфигурации недоступен. Настройки не сохранены.")
            self._log_history("Менеджер конфигурации недоступен. Настройки не сохранены.", "WARNING")
            return

        db_params = {k: v for k, v in params.items() if k != 'table_name'}
        table_name = params.get('table_name', 'app_table') or 'app_table'

        try:
            cm = self.pipeline.config_manager
            # Если есть метод set — используем, иначе логируем ошибку
            if hasattr(cm, "set") and callable(getattr(cm, "set")):
                cm.set('database', db_params)
            else:
                logger.warning("config_manager не реализует метод set. Параметры не сохранены в конфиг.")
            # Для имени таблицы используем set_setting если есть, иначе сохраняем в ключ 'db.table_name'
            if hasattr(cm, "set_setting") and callable(getattr(cm, "set_setting")):
                cm.set_setting('db.table_name', table_name)
            elif hasattr(cm, "set") and callable(getattr(cm, "set")):
                cm.set('db.table_name', table_name)
            messagebox.showinfo("Конфигурация", "Настройки базы данных и имя таблицы сохранены.")
            self._log_history("Настройки БД и имя таблицы сохранены в конфиг.", "INFO")
        except Exception as e:
            logger.exception(f"Ошибка при сохранении конфигурации: {e}")
            messagebox.showerror("Ошибка сохранения конфига", f"Не удалось сохранить настройки: {e}")
            self._log_history(f"Ошибка сохранения настроек БД: {e}", "ERROR")

    def on_save_to_db(self):
        """Сохранить данные из pipeline в БД через pipeline.save_to_db()."""
        if isinstance(self.pipeline, Mock):
            messagebox.showwarning("Внимание", "Пайплайн не инициализирован. Сохранение в БД недоступно.")
            self._log_history("Пайплайн не инициализирован. Сохранение в БД недоступно.", "WARNING")
            return

        df = getattr(self.pipeline, "current_df", None)
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            messagebox.showwarning("Нет данных", "Нет данных в пайплайне для сохранения в БД.")
            self._log_history("Нет данных для сохранения в БД.", "WARNING")
            return

        params = self.get_params()
        table_name = params.get('table_name', '').strip()
        if not table_name:
            messagebox.showerror("Ошибка", "Пожалуйста, укажите имя таблицы для сохранения.")
            self._log_history("Ошибка: не указано имя таблицы для сохранения в БД.", "ERROR")
            return

        db_connection_params = {k: params[k] for k in ['host', 'port', 'user', 'password', 'database'] if params.get(k)}

        try:
            if hasattr(self.pipeline, "save_to_db") and callable(getattr(self.pipeline, "save_to_db")):
                self.pipeline.save_to_db(table_name=table_name, connection_params=db_connection_params)
                messagebox.showinfo("Успех", f"Данные успешно сохранены в таблицу '{table_name}'.")
                self._log_history(f"Данные сохранены в таблицу '{table_name}' в БД.", "INFO")
            else:
                raise RuntimeError("pipeline не реализует save_to_db.")
        except Exception as e:
            logger.exception(f"Ошибка при сохранении в БД: {e}")
            messagebox.showerror("Ошибка", f"Не удалось сохранить данные: {e}")
            self._log_history(f"Ошибка при сохранении в БД: {e}", "ERROR")

    def on_load_from_db(self):
        """
        Загружает данные из БД, используя pipeline.load_from_db(),
        и обновляет текущий DataFrame в pipeline и GUI.
        """
        if isinstance(self.pipeline, Mock):
            messagebox.showwarning("Внимание", "Пайплайн не инициализирован. Загрузка из БД недоступна.")
            self._log_history("Пайплайн не инициализирован. Загрузка из БД недоступна.", "WARNING")
            return

        params = self.get_params()
        table_name = params.get('table_name', '').strip()
        if not table_name:
            messagebox.showerror("Ошибка", "Пожалуйста, укажите имя таблицы для загрузки.")
            self._log_history("Ошибка: не указано имя таблицы для загрузки из БД.", "ERROR")
            return

        db_connection_params = {k: params[k] for k in ['host', 'port', 'user', 'password', 'database'] if params.get(k)}

        self._log_history(f"Запрос на загрузку данных из таблицы '{table_name}' из БД.", "INFO")

        try:
            if not hasattr(self.pipeline, "load_from_db") or not callable(getattr(self.pipeline, "load_from_db")):
                raise RuntimeError("pipeline не реализует load_from_db.")

            df = self.pipeline.load_from_db(table_name=table_name, connection_params=db_connection_params)

            # Обработка случая, когда pipeline возвращает None или не DataFrame
            if df is None:
                self._log_history("Загрузка из БД вернула None. Обрабатываем как пустой DataFrame.", "WARNING")
                df = pd.DataFrame()

            if not isinstance(df, pd.DataFrame):
                self._log_history("Загруженные данные не являются pandas.DataFrame. Преобразуем в пустой DataFrame.", "ERROR")
                df = pd.DataFrame()

            messagebox.showinfo("Готово", f"Данные успешно загружены из таблицы '{table_name}'. Количество строк: {len(df)}")
            self._log_history(f"Данные загружены из БД. Размер DataFrame: {df.shape}", "INFO")

            # Обновляем pipeline.current_df только если pipeline поддерживает это
            try:
                setattr(self.pipeline, "current_df", df)
            except Exception:
                logger.debug("Не удалось установить pipeline.current_df напрямую.")

            self.update_data_display(df)
        except ValueError as ve:
            messagebox.showerror("Ошибка", f"Ошибка данных или параметров подключения: {ve}")
            self._log_history(f"Ошибка при загрузке из БД (ValueError): {ve}", "ERROR")
            self.update_data_display(None)
        except ConnectionError as ce:
            messagebox.showerror("Ошибка подключения", str(ce))
            self._log_history(f"Ошибка подключения к БД при загрузке: {ce}", "ERROR")
            self.update_data_display(None)
        except Exception as e:
            logger.exception(f"Не удалось загрузить данные из БД: {e}")
            messagebox.showerror("Ошибка", f"Не удалось загрузить данные: {e}")
            self._log_history(f"Не удалось загрузить данные из БД: {e}", "ERROR")
            self.update_data_display(None)

    def on_send_for_analysis(self):
        """Отправить данные на анализ (вызываем run_full_analysis пайплайна)."""
        if isinstance(self.pipeline, Mock):
            messagebox.showwarning("Внимание", "Пайплайн не инициализирован. Анализ недоступен.")
            self._log_history("Пайплайн не инициализирован. Анализ недоступен.", "WARNING")
            return

        df = getattr(self.pipeline, "current_df", None)
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            messagebox.showwarning("Нет данных", "Нет данных в пайплайне для анализа. Загрузите данные сначала.")
            self._log_history("Нет данных для анализа. Загрузите данные сначала.", "WARNING")
            return

        self._log_history("Начат анализ данных...", "INFO")
        try:
            if not hasattr(self.pipeline, "run_full_analysis") or not callable(getattr(self.pipeline, "run_full_analysis")):
                raise RuntimeError("pipeline не реализует run_full_analysis.")

            analysis_results = self.pipeline.run_full_analysis()
            messagebox.showinfo("Анализ", "Анализ данных завершен! Результаты доступны во вкладке 'Визуализация' или в логах.")
            self._log_history("Анализ данных завершен.", "INFO")
        except Exception as e:
            logger.exception(f"Ошибка при анализе данных: {e}")
            messagebox.showerror("Ошибка анализа", f"Не удалось выполнить анализ: {e}")
            self._log_history(f"Ошибка при анализе данных: {e}", "ERROR")

    def update_data_display(self, df: Optional[pd.DataFrame]):
        """
        Обновляет GUI-виджеты для отображения сводки и предпросмотра DataFrame.
        Если df=None или пустой, очищает отображение.
        """
        if self.data_preview_text is None or self.data_summary_label is None:
            logger.debug("Виджеты предпросмотра данных не инициализированы.")
            return

        self.data_preview_text.config(state="normal")
        self.data_preview_text.delete(1.0, tk.END)

        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
            rows, cols = df.shape
            self.data_summary_label.config(text=f"Загружено: {rows} строк, {cols} столбцов")
            try:
                preview_str = df.head(10).to_string()
            except Exception:
                preview_str = str(df.head(10))
            self.data_preview_text.insert(tk.END, preview_str)
        else:
            self.data_summary_label.config(text="Нет загруженных данных")
            self.data_preview_text.insert(tk.END, "Здесь будет предпросмотр данных после загрузки из БД.")

        self.data_preview_text.config(state="disabled")
        logger.debug("GUI отображение данных обновлено.")
