# config/logging_config.py
import os
import logging
from logging.handlers import RotatingFileHandler

# Убедимся, что директория для логов существует
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s'
        },
        'gui_display': { # Форматтер для GUI
            'format': '%(asctime)s [%(levelname)s] %(message)s',
            'datefmt': '%H:%M:%S'
        }
    },
    'handlers': {
        'console': { # Вывод в консоль
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO',
        },
        'app_file': { # Основной лог приложения
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(LOG_DIR, 'app.log'),
            'maxBytes': 10485760, # 10 MB
            'backupCount': 5,
            'formatter': 'detailed',
            'encoding': 'utf-8',
        },
        'gui_file': { # Лог событий GUI в отдельный файл
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(LOG_DIR, 'gui_events.log'),
            'maxBytes': 2097152,  # 2MB
            'backupCount': 2,
            'formatter': 'standard', # Используем стандартный форматтер
            'encoding': 'utf-8',
        },
        # TkinterLogHandler будет добавлен программно в gui_app.py
        # и будет привязан к логгеру 'gui_output'
    },
    'loggers': {
        # Корневой логгер: для общих сообщений, которые не перехватываются дочерними
        # Он будет писать в консоль и app.log
        '': {
            'handlers': ['console', 'app_file'],
            'level': 'INFO',
            'propagate': True # Сообщения от корневого логгера передаются дальше (если есть родительские, но здесь нет)
        },
        # Логгер для внутренней логики GUI (например, инициализация виджетов)
        'gui': {
            'handlers': ['console', 'gui_file'], # Пишет в консоль и gui_events.log
            'level': 'INFO',
            'propagate': False # НЕ передавать сообщения в корневой логгер, чтобы избежать дублирования в консоли
        },
        # НОВЫЙ логгер СПЕЦИАЛЬНО для вывода в текстовое поле GUI
        'gui_output': {
            'handlers': [], # Обработчик будет добавлен программно в gui_app.py
            'level': 'INFO',
            'propagate': False # НЕ передавать сообщения в корневой логгер, чтобы избежать дублирования в консоли
        },
        # Логгеры для ваших модулей (data_processor, utils, analyzer)
        # Они будут писать в консоль и app.log
        'data_processor': {
            'handlers': ['console', 'app_file'],
            'level': 'INFO',
            'propagate': False
        },
        'utils': {
            'handlers': ['console', 'app_file'],
            'level': 'INFO',
            'propagate': False
        },
        'analysis': {
            'handlers': ['console', 'app_file'],
            'level': 'INFO',
            'propagate': False
        }
    }
}