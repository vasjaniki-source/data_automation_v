import tkinter as tk
from tkinter import ttk, messagebox
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

class DBConfigWidget(ttk.Frame):
    def __init__(self, parent, pipeline):
        super().__init__(parent, padding="20")
        self.pipeline = pipeline
        self.create_widgets()

    def create_widgets(self):
        ttk.Label(self, text="Настройка подключения PostgreSQL", font=('Arial', 12, 'bold')).grid(row=0, column=0, columnspan=2, pady=10)

        fields = [
            ("Хост:", "host"), ("Порт:", "port"), 
            ("Пользователь:", "user"), ("Пароль:", "password"), 
            ("База данных:", "database")
        ]
        
        self.entries = {}
        for i, (label, key) in enumerate(fields, 1):
            ttk.Label(self, text=label).grid(row=i, column=0, sticky="w", pady=5)
            show = "*" if key == "password" else ""
            ent = ttk.Entry(self, show=show, width=30)
            ent.grid(row=i, column=1, sticky="ew", padx=5)
            self.entries[key] = ent

        # Загрузка текущих настроек
        conf = self.pipeline.config_manager.get('database', {})
        for key, entry in self.entries.items():
            entry.insert(0, str(conf.get(key, "")))

        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=6, column=0, columnspan=2, pady=20)
        
        ttk.Button(btn_frame, text="Проверить подключение", command=self.test_connection).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Сохранить конфиг", command=self.save_config).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Сохранить в БД", command=self.on_save_to_db).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Загрузить из БД", command=self.on_load_from_db).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Отправить на анализ", command=self.on_send_for_analysis).pack(side="left", padx=5)

        # --- раздел для отображения загруженных данных ---
        data_display_frame = ttk.LabelFrame(self, text="Загруженные данные", padding="10")
        data_display_frame.grid(row=7, column=0, columnspan=2, sticky="ew", pady=10)
        
        self.data_summary_label = ttk.Label(data_display_frame, text="Нет данных")
        self.data_summary_label.pack(fill="x", pady=5)

        self.data_preview_text = tk.Text(data_display_frame, height=8, width=50, state="disabled", wrap="word")
        self.data_preview_text.pack(fill="both", expand=True)

        # Добавим скроллбар для Text виджета
        scrollbar = ttk.Scrollbar(data_display_frame, command=self.data_preview_text.yview)
        scrollbar.pack(side="right", fill="y")
        self.data_preview_text.config(yscrollcommand=scrollbar.set)
        
        # Настроим растягивание колонок
        self.grid_columnconfigure(1, weight=1)
        data_display_frame.grid_columnconfigure(0, weight=1)

    def get_params(self):
        return {k: v.get().strip() for k, v in self.entries.items()}

    def test_connection(self):
        params = self.get_params()
        if not params['port'].isdigit():
            messagebox.showerror("Ошибка", "Порт должен быть числом")
            return
        
        try:
            # Вызов через обработчик БД в пайплайне
            self.pipeline.db_handler.connection_params = params
            # Предполагаем наличие метода test_connection в PostgresHandler
            conn = self.pipeline.db_handler._connect()
            conn.close()
            messagebox.showinfo("Успех", "Подключение установлено!")
            logger.info("Успешная проверка подключения к БД")
        except Exception as e:
            messagebox.showerror("Ошибка подключения", str(e))
            logger.error(f"Ошибка проверки БД: {e}")

    def save_config(self):
        params = self.get_params()
        self.pipeline.config_manager.set('database', params)
        messagebox.showinfo("Конфигурация", "Настройки базы данных сохранены.")

    def _collect_dataframe_from_pipeline(self):
        """Пытаемся получить DataFrame из pipeline разными способами. Возвращает pandas.DataFrame или None."""
        try:
            import pandas as pd
        except Exception:
            return None

        src = None
        if hasattr(self.pipeline, 'get_current_dataframe'):
            src = self.pipeline.get_current_dataframe()
        elif hasattr(self.pipeline, 'get_dataframe'):
            src = self.pipeline.get_dataframe()
        elif hasattr(self.pipeline, 'current_df'):
            src = getattr(self.pipeline, 'current_df')
        elif hasattr(self.pipeline, 'dataframe'):
            src = getattr(self.pipeline, 'dataframe')
        elif hasattr(self.pipeline, 'data'):
            src = getattr(self.pipeline, 'data')

        if src is None:
            return None

        if isinstance(src, pd.DataFrame):
            return src

        try:
            df = pd.DataFrame(src)
            return df
        except Exception:
            return None

    def on_save_to_db(self):
        """Сохранить данные из pipeline в БД через pipeline.db_handler"""
        df = self._collect_dataframe_from_pipeline()
        if df is None or df.empty:
            messagebox.showwarning("Нет данных", "Не удалось получить данные для сохранения в БД.")
            return

        if not hasattr(self.pipeline, 'db_handler'):
            messagebox.showerror("Ошибка", "DB handler не найден в pipeline.")
            return

        try:
            params = self.pipeline.config_manager.get('database', {})
            if params:
                try:
                    self.pipeline.db_handler.connection_params = params
                except Exception:
                    logger.debug('Не удалось установить connection_params у db_handler')

            table_name = getattr(self.pipeline, 'db_table_name', 'app_table')
            self.pipeline.db_handler.save_dataframe_to_table(df, table_name)
            messagebox.showinfo("Успех", "Данные успешно сохранены в базу данных.")
        except Exception as e:
            logger.error(f"Ошибка при сохранении в БД: {e}", exc_info=True)
            messagebox.showerror("Ошибка", f"Не удалось сохранить данные: {e}")

    def on_load_from_db(self):
        """
        Загружает данные из БД, используя pipeline.load_from_db(),
        и обновляет текущий DataFrame в pipeline.
        """
        # Проверяем, что pipeline и его необходимые компоненты существуют
        if not hasattr(self.pipeline, 'load_from_db'):
            messagebox.showerror("Ошибка", "Метод 'load_from_db' не найден в pipeline.")
            logger.error("Метод 'load_from_db' отсутствует в pipeline.")
            return
        
        # Получаем параметры подключения из конфиг-менеджера.
        # Сам pipeline.load_from_db позаботится об их валидации и установке в db_handler.
        connection_params = self.pipeline.config_manager.get('database', {})
        if 'port' in connection_params:
            try:
                # Проверяем, является ли порт уже числом, если нет, пытаемся преобразовать
                if not isinstance(connection_params['port'], int):
                    connection_params['port'] = int(connection_params['port'])
            except (ValueError, TypeError): # Ловим ошибки, если порт не число и не может быть преобразован
                messagebox.showerror("Ошибка", "Порт в настройках должен быть числом.")
                logger.error("Порт в настройках БД не является корректным числом.")
                self.update_data_display(None) # Очищаем отображение при ошибке
                return
        # Получаем имя таблицы. Если оно не установлено в pipeline, используем дефолт.
        table_name = getattr(self.pipeline, 'db_table_name', 'app_table')
        
        # Можем также определить лимит, если он установлен в pipeline.
        # Например:
        # limit = getattr(self.pipeline, 'db_load_limit', None)
        
        logger.info(f"Запрос на загрузку данных из таблицы '{table_name}' из БД.")

        try:
            # Вызываем новый метод pipeline для загрузки данных.
            # Этот метод сам позаботится о получении параметров подключения
            # из config_manager (если не переданы явно) и их установке.
            df = self.pipeline.load_from_db(
                table_name=table_name,
                connection_params=connection_params # Передаем параметры, полученные из config_manager
                # limit=limit # Раскомментировать, если используется параметр limit
            )
            
            # Сообщение об успехе генерируется внутри pipeline.load_from_db, 
            # но здесь мы можем добавить более специфичное для GUI.
            messagebox.showinfo("Готово", f"Данные успешно загружены из таблицы '{table_name}'. Количество строк: {len(df)}")
            logger.info(f"Данные загружены из БД. Размер DataFrame: {df.shape}")

            try:
                # Вызываем новый метод pipeline для загрузки данных.
                df = self.pipeline.load_from_db(
                    table_name=table_name,
                    connection_params=connection_params 
                )
                
                messagebox.showinfo("Готово", f"Данные успешно загружены из таблицы '{table_name}'. Количество строк: {len(df)}")
                logger.info(f"Данные загружены из БД. Размер DataFrame: {df.shape}")

                # --- Вот здесь вызываем новую логику обновления GUI ---
                self.update_data_display(df)
                # --- Конец новой логики ---

            except ValueError as ve:
                # ... (обработка ошибок)
                self.update_data_display(None) # Очищаем отображение при ошибке
            except ConnectionError as ce:
                # ... (обработка ошибок)
                self.update_data_display(None) # Очищаем отображение при ошибке
            except Exception as e:
                # ... (обработка ошибок)
                self.update_data_display(None) # Очищаем отображение при ошибке

        except ValueError as ve:
            # Обработка ошибок, связанных с параметрами подключения или отсутствием данных
            messagebox.showerror("Ошибка", f"Ошибка данных или параметров подключения: {ve}")
            logger.error(f"Ошибка при загрузке из БД (ValueError): {ve}", exc_info=True)
        except ConnectionError as ce:
            # Обработка ошибок установки соединения
            messagebox.showerror("Ошибка подключения", str(ce))
            logger.error(f"Ошибка подключения к БД при загрузке: {ce}", exc_info=True)
        except Exception as e:
            # Обработка прочих ошибок
            logger.error(f"Не удалось загрузить данные из БД: {e}", exc_info=True)
            messagebox.showerror("Ошибка", f"Не удалось загрузить данные: {e}")
            
    def on_send_for_analysis(self):
        """Отправить данные на анализ (пытаемся вызвать методы pipeline)."""
        df = self._collect_dataframe_from_pipeline()
        if df is None or df.empty:
            messagebox.showwarning("Нет данных", "Не удалось получить данные для анализа.")
            return

        try:
            if hasattr(self.pipeline, 'analyze_data'):
                res = self.pipeline.analyze_data(df)
                messagebox.showinfo("Анализ", f"Результат анализа: {res}")
                return
            if hasattr(self.pipeline, 'send_to_analyzer'):
                res = self.pipeline.send_to_analyzer(df)
                messagebox.showinfo("Анализ", "Данные отправлены на анализ.")
                return

            setattr(self.pipeline, 'analysis_input', df)
            messagebox.showinfo("Анализ", "Данные подготовлены для анализа (pipeline.analysis_input).")
        except Exception as e:
            logger.error(f"Ошибка при отправке на анализ: {e}", exc_info=True)
            messagebox.showerror("Ошибка", f"Не удалось отправить на анализ: {e}")

    def update_data_display(self, df: Optional[pd.DataFrame]):
        """
        Обновляет GUI-виджеты для отображения сводки и предпросмотра DataFrame.
        Если df=None или пустой, очищает отображение.
        """
        self.data_preview_text.config(state="normal") # Разрешаем редактирование для обновления
        self.data_preview_text.delete(1.0, tk.END) # Очищаем предыдущий текст

        if df is not None and not df.empty:
            rows, cols = df.shape
            self.data_summary_label.config(text=f"Загружено: {rows} строк, {cols} столбцов")
            
            # Показываем первые 5 строк DataFrame
            preview_str = df.head().to_string(index=False)
            self.data_preview_text.insert(tk.END, preview_str)
        else:
            self.data_summary_label.config(text="Нет загруженных данных")
            self.data_preview_text.insert(tk.END, "Здесь будет предпросмотр данных после загрузки из БД.")
        
        self.data_preview_text.config(state="disabled") # Запрещаем редактирование
        logger.debug("GUI отображение данных обновлено.")
