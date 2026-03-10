import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import logging

logger = logging.getLogger(__name__)

class FileSelectorWidget(ttk.Frame):
    def __init__(self, parent, pipeline, log_callback):
        super().__init__(parent, padding="20")
        self.pipeline = pipeline
        self.log_callback = log_callback
        self.create_widgets()

    def create_widgets(self):
        # Выбор источника
        source_frame = ttk.LabelFrame(self, text="Источник данных", padding="10")
        source_frame.pack(fill="x", pady=10)

        self.source_var = tk.StringVar(value="CSV/Excel")
        sources = ["CSV/Excel", "API", "SQL"]
        for s in sources:
            ttk.Radiobutton(source_frame, text=s, variable=self.source_var, 
                            value=s, command=self.toggle_inputs).pack(side="left", padx=10)

        # Контейнер для динамических полей
        self.input_container = ttk.Frame(self)
        self.input_container.pack(fill="x", pady=10)
        
        self.toggle_inputs()

        # Кнопка загрузки
        ttk.Button(self, text="📥 Загрузить данные в систему", 
                   command=self.load_data).pack(pady=20)

        # Инфо-панель
        self.info_label = ttk.Label(self, text="Данные не загружены", foreground="gray")
        self.info_label.pack()

    def toggle_inputs(self):
        for widget in self.input_container.winfo_children():
            widget.destroy()

        src = self.source_var.get()
        if src == "CSV/Excel":
            f = ttk.Frame(self.input_container)
            f.pack(fill="x")
            self.path_ent = ttk.Entry(f)
            self.path_ent.pack(side="left", fill="x", expand=True, padx=(0, 5))
            ttk.Button(f, text="Обзор", command=self.browse_file).pack(side="right")
        elif src == "API":
            ttk.Label(self.input_container, text="URL API:").pack(anchor="w")
            self.api_ent = ttk.Entry(self.input_container)
            self.api_ent.pack(fill="x", pady=5)
        elif src == "SQL":
            ttk.Label(self.input_container, text="SQL Запрос:").pack(anchor="w")
            self.sql_ent = tk.Text(self.input_container, height=4)
            self.sql_ent.pack(fill="x", pady=5)

    def browse_file(self):
        path = filedialog.askopenfilename(filetypes=[("Data files", "*.csv *.xlsx *.xls")])
        if path:
            self.path_ent.delete(0, tk.END)
            self.path_ent.insert(0, path)

    def load_data(self):
        src = self.source_var.get().lower()
        if "csv" in src: src = "csv/excel"
        
        try:
            params = {}
            if src == "csv/excel":
                params['file_path'] = self.path_ent.get()
            elif src == "api":
                params['api_url'] = self.api_ent.get()
            
            df = self.pipeline.load_data(src, **params)
            self.info_label.config(text=f"Загружено: {df.shape[0]} строк, {df.shape[1]} колонок", foreground="green")
            self.log_callback(f"Успешная загрузка из {src}", "SUCCESS")
        except Exception as e:
            messagebox.showerror("Ошибка загрузки", str(e))
            self.log_callback(f"Ошибка загрузки: {e}", "ERROR")