import tkinter as tk
from tkinter import ttk
from unittest.mock import Mock
from .widgets import DBConfigWidget, FileSelectorWidget, HistoryPanelWidget, VisualizationPanelWidget, SettingsPanelWidget
from data_processor import DataProcessingPipeline
from utils.config_manager import ConfigManager

class DataAutomationGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Automation System v5.0")
        self.root.geometry("1100x850")

        # Инициализация логики
        self.config_manager = ConfigManager()
        self.pipeline = DataProcessingPipeline(self.config_manager)
        self.mock_pipeline = Mock()
        self.create_layout()

    def create_layout(self):
        # Вкладки
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=5)

        # Инициализация виджетов
        self.history_tab = HistoryPanelWidget(self.notebook, self.pipeline)
        
        # Передаем callback для записи в историю из других вкладок
        self.load_tab = FileSelectorWidget(self.notebook, self.pipeline, self.add_to_history)
        self.db_tab = DBConfigWidget(self.notebook, self.pipeline)
        self.viz_tab = VisualizationPanelWidget(self.notebook, self.pipeline)
        self.settings_tab = SettingsPanelWidget(self.notebook, self.pipeline)

        self.notebook.add(self.load_tab, text="📥 Загрузка")
        self.notebook.add(self.db_tab, text="🗄️ База данных")
        self.notebook.add(self.viz_tab, text="📊 Визуализация")
        self.notebook.add(self.history_tab, text="📜 История")
        self.notebook.add(self.settings_tab, text="⚙️ Настройки")

        # Панель логов (внизу)
        log_frame = ttk.LabelFrame(self.root, text="Системный лог", padding="5")
        log_frame.pack(fill="x", padx=10, pady=5)
        
        self.log_text = tk.Text(log_frame, height=6, state="disabled", font=("Consolas", 9))
        self.log_text.pack(fill="x", side="left", expand=True)
        
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side="right", fill="y")
        self.log_text.config(yscrollcommand=scrollbar.set)

    def add_to_history(self, msg, status="INFO"):
        """Метод для связи между виджетами через главный класс"""
        # Запись в Treeview
        self.history_tab.add_entry(msg.split(":")[0], status, msg)
        
        # Запись в лог-панель
        self.log_text.config(state="normal")
        self.log_text.insert(tk.END, f"[{status}] {msg}\n")
        self.log_text.config(state="disabled")
        self.log_text.see(tk.END)

if __name__ == "__main__":
    root = tk.Tk()
    app = DataAutomationGUI(root)
    root.mainloop()
