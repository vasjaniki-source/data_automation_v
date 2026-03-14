import json
import os
import logging
from typing import Any
import logging.config

# --- Загрузка переменных окружения из .env файла ---
from dotenv import load_dotenv
load_dotenv() 

try:
    from config.logging_config import LOGGING_CONFIG
    logging.config.dictConfig(LOGGING_CONFIG)
except ImportError:
    logging.basicConfig(level=logging.INFO)

class ConfigManager:
    """
    Класс для управления конфигурацией приложения.
    """

    def __init__(self, config_file="config/default_settings.json"):
        self.config_file = config_file
        
        # Конфигурация по умолчанию
        self._defaults = {
            'database': {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 5432)), 
                'database': os.getenv('DB_NAME', 'postgres'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', ''),
                'default_table': 'imported_data'
            },
            'file_settings': {
                'max_file_size_mb': int(os.getenv('MAX_FILE_SIZE_MB', 100)), 
                'allowed_extensions': ['.csv', '.xlsx', '.xls', '.data', '.txt'] # Добавьте сюда, если нужно
            },
            'smtp': {
                'host': os.getenv('SMTP_HOST', 'smtp.mail.ru'),
                'port': int(os.getenv('SMTP_PORT', 587)), 
                'user': os.getenv('SMTP_USER', 'nikitkov.vasya@mail.ru'),
                'password': os.getenv('SMTP_PASSWORD'), # Без дефолта, если пароль обязателен
                'use_tls': True,
                'send_email': bool(os.getenv('SMTP_SEND_EMAIL', 'False').lower() == 'true'), # Читаем булево значение
                'recipients': os.getenv('SMTP_RECIPIENTS', '') # Читаем список получателей
            },
            'app': {
                'report_dir': 'reports',
                'log_level': 'INFO'
            }
        }

        # Загружаем конфигурацию
        self._config = self.load_config()

    def load_config(self) -> dict:
        """Загружает конфиг из файла, дополняя его значениями по умолчанию."""
        config = self._defaults.copy()

        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                
                # Глубокое обновление (Deep Update)
                for section, values in loaded_data.items():
                    if section in config and isinstance(values, dict):
                        config[section].update(values)
                    else:
                        config[section] = values
                
                logging.info(f"Конфигурация загружена из {self.config_file}")
            except Exception as e:
                logging.error(f"Ошибка чтения файла конфигурации: {e}")
        else:
            logging.warning(f"Файл {self.config_file} не найден. Создаю новый с дефолтными настройками.")
            self.save_config(config)

        return config

    def save_config(self, config: dict | None = None):
        """Сохраняет текущую конфигурацию в JSON файл."""
        data_to_save = config if config is not None else self._config
        try:
            # Создаем папку, если её нет
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=4, ensure_ascii=False)
            logging.info(f"Конфигурация сохранена в {self.config_file}")
        except Exception as e:
            logging.error(f"Ошибка при сохранении конфигурации: {e}")

    def get(self, section: str, default: Any = None) -> Any:
        """Безопасно возвращает секцию."""
        return self._config.get(section, default)

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Получение настройки через точку: 'smtp.host'"""
        keys = key.split('.')
        current = self._config
        try:
            for k in keys:
                current = current[k]
            return current
        except (KeyError, TypeError):
            return default

    def set(self, section: str, values: Any):
        """Обновляет настройку в памяти."""
        if isinstance(values, dict) and section in self._config and isinstance(self._config[section], dict):
            self._config[section].update(values)
        else:
            self._config[section] = values