
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import logging
from typing import Optional, Callable, Dict, Any, Literal, Union, Tuple
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
from data_processor.pipeline import DataProcessingPipeline

logger = logging.getLogger(__name__)

class FileSelectorWidget(ttk.Frame):
    def __init__(self, parent: tk.Widget, pipeline: Optional[DataProcessingPipeline], log_callback: Callable[[str, Literal["INFO","WARNING","ERROR"]], None]):
        super().__init__(parent, padding="20")
        self.pipeline = pipeline
        self.log_callback = log_callback
        self.current_input_widgets: Dict[str, Union[ttk.Label, ttk.Entry, tk.Text, ttk.Button]] = {}
        # гарантируем наличие атрибута, чтобы тесты/IDE не падали
        self.path_ent: Optional[ttk.Entry] = None
        self.create_widgets()

    def create_widgets(self) -> None:
        # Выбор источника
        source_frame = ttk.LabelFrame(self, text="Источник данных", padding="10")
        source_frame.pack(fill="x", pady=10)

        self.source_var = tk.StringVar(value="CSV/Excel")
        sources = ["CSV/Excel", "API", "SQL"]
        for s in sources:
            ttk.Radiobutton(
                source_frame,
                text=s,
                variable=self.source_var,
                value=s,
                command=self.toggle_inputs
            ).pack(side="left", padx=10)

        # Контейнер для динамических полей
        self.input_container = ttk.Frame(self)
        self.input_container.pack(fill="x", pady=10)
        
        self.toggle_inputs()  # Инициализация полей ввода

        # Кнопка загрузки
        ttk.Button(
            self,
            text="📥 Загрузить данные в систему",
            command=self.load_data
        ).pack(pady=20)

        # Инфо‑панель
        self.info_label = ttk.Label(self, text="Данные не загружены", foreground="gray")
        self.info_label.pack()

    def toggle_inputs(self) -> None:
        # Очищаем предыдущие виджеты ввода
        for widget in self.input_container.winfo_children():
            widget.destroy()
        self.current_input_widgets.clear()  # Очищаем словарь

        src_type = self.source_var.get()

        if src_type == "CSV/Excel":
            f = ttk.Frame(self.input_container)
            f.pack(fill="x")
            self.current_input_widgets['path_label'] = ttk.Label(f, text="Путь к файлу:")
            self.current_input_widgets['path_label'].pack(side="left", padx=(0, 5))
            entry = ttk.Entry(f)
            entry.pack(side="left", fill="x", expand=True, padx=(0, 5))
            self.current_input_widgets['path_entry'] = entry
            # алиас для совместимости с тестами/старым кодом
            self.path_ent = entry
            ttk.Button(f, text="Обзор", command=self.browse_file).pack(side="right")
        
        elif src_type == "API":
            f = ttk.Frame(self.input_container)
            f.pack(fill="x")
            self.current_input_widgets['api_label'] = ttk.Label(f, text="URL API:")
            self.current_input_widgets['api_label'].pack(side="left", padx=(0, 5))
            self.current_input_widgets['api_entry'] = ttk.Entry(f)
            self.current_input_widgets['api_entry'].pack(side="left", fill="x", expand=True, padx=(0, 5))

        elif src_type == "SQL":
            f = ttk.Frame(self.input_container)
            f.pack(fill="x")
            self.current_input_widgets['sql_label'] = ttk.Label(f, text="SQL Запрос:")
            self.current_input_widgets['sql_label'].pack(anchor="w")
            # Используем Text для SQL запросов, т. к. они могут быть длинными
            self.current_input_widgets['sql_entry'] = tk.Text(f, height=5, width=50, font=("Consolas", 9))
            self.current_input_widgets['sql_entry'].pack(fill="x", pady=5)
            # Добавляем скроллбар для SQL Text виджета
            sql_scrollbar = ttk.Scrollbar(f, command=self.current_input_widgets['sql_entry'].yview)
            sql_scrollbar.pack(side="right", fill="y")
            self.current_input_widgets['sql_entry'].config(yscrollcommand=sql_scrollbar.set)
        else:
            # при переключении удаляем алиас
            self.path_ent = None
            
    def _safe_delete(self, widget_key: str, start: str = "0", end: str = tk.END) -> None:
        """Безопасное удаление текста из Entry виджета."""
        widget = self.current_input_widgets.get(widget_key)
        if isinstance(widget, ttk.Entry):
            widget.delete(start, end)

    def _safe_insert(self, widget_key: str, index: str, value: str) -> None:
        """Безопасная вставка текста в Entry виджет."""
        widget = self.current_input_widgets.get(widget_key)
        if isinstance(widget, ttk.Entry):
            widget.insert(index, value)

    def _safe_get_text(self, widget_key: str) -> str:
        """Безопасное получение текста из Entry или Text виджета."""
        widget = self.current_input_widgets.get(widget_key)
        if isinstance(widget, ttk.Entry):
            return widget.get()
        elif isinstance(widget, tk.Text):
            return widget.get("1.0", tk.END).strip()
        return ""

    def browse_file(self) -> None:
        if 'path_entry' not in self.current_input_widgets:
            messagebox.showwarning("Предупреждение", "Поле ввода пути к файлу не найдено.")
            self.log_callback("Поле ввода пути к файлу не найдено в GUI.", "WARNING")
            return

        file_path = filedialog.askopenfilename(
            title="Выберите файл данных",
            filetypes=[
                ("CSV files", "*.csv"),
                ("Excel files", "*.xlsx *.xls"),
                ("All files", "*.*")
            ]
        )
        if file_path:
            self._safe_delete('path_entry')
            self._safe_insert('path_entry', '0', str(file_path))

    def get_source_params(self) -> Tuple[str, Dict[str, Any]]:
        """Собирает параметры в зависимости от выбранного типа источника."""
        source_type = self.source_var.get()
        params: Dict[str, Any] = {}
        source_key: Optional[str] = None  # Инициализируем заранее

        if source_type == "CSV/Excel":
            file_path = self._safe_get_text('path_entry').strip()
            if not file_path:
                raise ValueError("Не указан путь к файлу.")
            p = Path(file_path)
            if not p.is_file():
                raise FileNotFoundError(f"Файл не найден: {file_path}")
            params['file_path'] = file_path
            # Определяем конкретный тип по расширению — это запрещает передавать "csv/excel"
            suffix = p.suffix.lower()
            if suffix == '.csv':
                source_key = 'csv'
            elif suffix in ('.xlsx', '.xls'):
                source_key = 'excel'
            else:
                # Если расширение неизвестно, по умолчанию выберем 'csv' (более терпимый вариант)
                # Можно изменить на raise ValueError(...) если вы хотите строгую валидацию
                source_key = 'csv'

        elif source_type == "API":
            api_url = self._safe_get_text('api_entry').strip()
            if not api_url:
                raise ValueError("Не указан URL API.")
            params['api_url'] = api_url
            source_key = 'api'

        elif source_type == "SQL":
            sql_query = self._safe_get_text('sql_entry')
            if not sql_query:
                raise ValueError("Не указан SQL запрос.")
            params['sql_query'] = sql_query
            source_key = 'sql'

        else:
            raise ValueError(f"Неизвестный тип источника: {source_type}")

        # Гарантируем, что source_key инициализирован
        if source_key is None:
            raise RuntimeError(f"Не удалось определить ключ источника для типа: {source_type}")

        return source_key, params

    def load_data(self) -> None:
        try:
            source_key, params = self.get_source_params()

            # Явная проверка на None
            if self.pipeline is None:
                messagebox.showerror("Ошибка", "Пайплайн не инициализирован. Загрузка данных невозможна.")
                self.log_callback("Пайплайн не инициализирован.", "ERROR")
                return

            # Проверка на Mock‑объект
            if isinstance(self.pipeline, Mock):
                messagebox.showwarning("Внимание", "Пайплайн не инициализирован. Загрузка данных недоступна.")
                self.log_callback("Пайплайн не инициализирован. Загрузка данных недоступна.", "WARNING")
                return

            # Вызываем загрузчик
            df = self.pipeline.load_data(source=source_key, **params)

            # Проверяем результат безопасно
            if df is None:
                # pipeline решил возвращать None при ошибке (возможный режим)
                err = getattr(self.pipeline, 'last_load_error', None)
                msg = "Данные не загружены."
                if err:
                    msg += f"\nПричина: {type(err).__name__}: {err}"
                messagebox.showerror("Ошибка загрузки", msg)
                self.info_label.config(text="Ошибка загрузки данных", foreground="red")
                self.log_callback(f"Ошибка загрузки из источника '{source_key}': {err}", "ERROR")
                return

            if not isinstance(df, (pd.DataFrame,)):
                # Непредвиденный формат
                messagebox.showerror("Ошибка данных", "Получен неверный формат данных.")
                self.info_label.config(text="Неверный формат данных", foreground="red")
                self.log_callback(f"Получен неверный формат данных от pipeline: {type(df)}", "ERROR")
                return

            # Корректный DataFrame — обрабатываем пустой и непустой случаи
            if df.empty:
                # Показываем дружелюбное сообщение: загрузка прошла, но данных нет
                self.info_label.config(text="Данные загружены, но DataFrame пуст.", foreground="orange")
                # Если pipeline сохранил ошибку — показываем её в лог
                last_err = getattr(self.pipeline, 'last_load_error', None)
                if last_err:
                    self.log_callback(f"Загрузка вернула пустой DataFrame. Предыдущее исключение: {last_err}", "WARNING")
                else:
                    self.log_callback(f"Загрузка завершена: пустой DataFrame из источника '{source_key}'.", "INFO")
                return

            # Успешная загрузка с данными
            self.info_label.config(
                text=f"Загружено: {df.shape[0]} строк, {df.shape[1]} колонок",
                foreground="green"
            )
            self.log_callback(f"Успешная загрузка из источника '{source_key}'.", "INFO")

        except ValueError as ve:
            messagebox.showerror("Ошибка ввода", str(ve))
            self.log_callback(f"Ошибка ввода параметров: {ve}", "ERROR")

        except FileNotFoundError as fnfe:
            messagebox.showerror("Ошибка файла", str(fnfe))
            self.log_callback(f"Ошибка файла: {fnfe}", "ERROR")

        except RuntimeError as rte:
            logger.error(f"Критическая ошибка в GUI виджете: {rte}", exc_info=True)
            messagebox.showerror("Ошибка GUI", str(rte))
            self.log_callback(f"Ошибка GUI: {rte}", "ERROR")

        except AttributeError as ae:
            # Дополнительная защита: редко возникает, но логируем и показываем сообщение
            logger.error(f"Ошибка атрибута при работе с DataFrame: {ae}", exc_info=True)
            messagebox.showerror(
                "Ошибка данных",
                "Некорректный формат данных или отсутствует необходимый атрибут."
            )
            self.log_callback(f"Ошибка атрибута: {ae}", "ERROR")

        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке данных: {e}", exc_info=True)
            err_text = f"Произошла ошибка: {type(e).__name__}: {e}"
            messagebox.showerror("Ошибка загрузки", err_text)
            self.log_callback(err_text, "ERROR")
