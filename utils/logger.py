import logging
import os
from logging.handlers import RotatingFileHandler

def setup_app_logging(log_level="INFO"):
    """Настройка глобального логирования для всего проекта."""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Основной лог приложения
    app_handler = RotatingFileHandler(
        os.path.join(log_dir, "app.log"), maxBytes=10**6, backupCount=5
    )
    app_handler.setFormatter(formatter)

    # Лог событий GUI
    gui_handler = RotatingFileHandler(
        os.path.join(log_dir, "gui_events.log"), maxBytes=10**6, backupCount=5
    )
    gui_handler.setFormatter(formatter)

    # Консольный вывод
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(app_handler)
    root_logger.addHandler(console_handler)

    # Специальный логгер для GUI
    gui_logger = logging.getLogger("gui")
    gui_logger.addHandler(gui_handler)
    
    return root_logger