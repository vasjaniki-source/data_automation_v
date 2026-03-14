
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
import csv
import logging
from typing import Callable, Optional

from sklearn.pipeline import Pipeline

logger = logging.getLogger(__name__)

class HistoryPanelWidget(ttk.Frame):
    def __init__(
        self,
        parent: tk.Widget,
        pipeline: Pipeline,
        log_callback: Optional[Callable[[str, str], None]] = None
    ):
        super().__init__(parent, padding="20")
        self.parent = parent
        self.pipeline = pipeline
        self.log_callback = log_callback
        self.create_widgets()
        # Начальная запись в историю
        self.add_entry("Инициализация GUI", "INFO", "Панель истории готова.", is_internal=True)

    def create_widgets(self):
        cols = ["time", "op", "status", "details"]
        self.tree = ttk.Treeview(self, columns=cols, show="headings")

        self.tree.heading("time", text="Время")
        self.tree.heading("op", text="Операция")
        self.tree.heading("status", text="Статус")
        self.tree.heading("details", text="Детали")

        # Настройка ширины колонок
        self.tree.column("time", width=120, minwidth=100, stretch=tk.NO)
        self.tree.column("op", width=150, minwidth=120, stretch=tk.NO)
        self.tree.column("status", width=100, minwidth=80, stretch=tk.NO)
        self.tree.column("details", width=400, minwidth=200, stretch=tk.YES)

        # Вертикальный скроллбар
        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        vsb.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=vsb.set)

        # Горизонтальный скроллбар
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        hsb.pack(side="bottom", fill="x")
        self.tree.configure(xscrollcommand=hsb.set)

        self.tree.pack(fill="both", expand=True, side="left")

        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", pady=10)

        ttk.Button(btn_frame, text="Очистить историю", command=self.clear_history).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Экспорт в CSV", command=self.export_history).pack(side="left", padx=5)

    def some_method(self):
        message = "Действие выполнено успешно"
        level = "INFO"
        if self.log_callback is not None:
            self.log_callback(message, level)

    def add_entry(self, operation: str, status: str, details: str, is_internal: bool = False):
        """Добавляет запись в Treeview истории."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.tree.insert("", "end", values=(timestamp, operation, status, details))
        self.tree.yview_moveto(1)  # Прокручиваем до конца

        if not is_internal:
            # Вызываем log_callback с двумя аргументами (сообщение и уровень)
            if self.log_callback is not None:
                self.log_callback(f"{operation}: {details}", status)
            logger.debug(f"Добавлена запись в историю: {operation}, Статус: {status}, Детали: {details}")

    def clear_history(self):
        if messagebox.askyesno("Очистить историю", "Вы уверены, что хотите полностью очистить историю?"):
            for item in self.tree.get_children():
                self.tree.delete(item)
            self.add_entry("Очистка истории", "INFO", "История успешно очищена.", is_internal=True)
            if self.log_callback is not None:
                self.log_callback("История успешно очищена.", "INFO")
            logger.info("История GUI очищена.")

    def export_history(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Экспорт истории"
        )
        if path:
            try:
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                # Записываем заголовки
                writer.writerow([self.tree.heading(col, "text") for col in self.tree["columns"]])
                for item in self.tree.get_children():
                    writer.writerow(self.tree.item(item)['values'])
                messagebox.showinfo("Экспорт", f"История успешно экспортирована в {path}")
                self.add_entry("Экспорт истории", "INFO", f"История экспортирована в {path}", is_internal=True)
                if self.log_callback is not None:
                    self.log_callback(f"История экспортирована в {path}", "INFO")
                logger.info(f"История GUI экспортирована в: {path}")
            except Exception as e:
                messagebox.showerror("Ошибка экспорта", f"Не удалось экспортировать историю: {e}")
                self.add_entry("Экспорт истории", "ERROR", f"Ошибка экспорта: {e}", is_internal=True)
                if self.log_callback is not None:
                    self.log_callback(f"Ошибка экспорта истории: {e}", "ERROR")
                logger.error(f"Ошибка экспорта истории GUI: {e}", exc_info=True)