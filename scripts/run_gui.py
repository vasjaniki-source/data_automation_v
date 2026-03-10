import sys
import os
import tkinter as tk

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gui.gui_app import DataAutomationGUI
from utils.logger import setup_app_logging

def main():
    # 1. Настройка логов
    setup_app_logging(log_level="INFO")
    
    # 2. Инициализация Tkinter
    root = tk.Tk()
    # 3. Запуск приложения
    app = DataAutomationGUI(root)
    gui_app = app  # Явное указание использования переменной (для Pylance)
    root.mainloop()

if __name__ == "__main__":
    main()