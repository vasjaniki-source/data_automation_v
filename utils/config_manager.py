import json
import os
import logging
from typing import Any

from config.logging_config import LOGGING_CONFIG
import logging.config

# Применить настройки логов
logging.config.dictConfig(LOGGING_CONFIG)

class ConfigManager:
    """
    Класс для управления конфигурацией приложения.
    Обеспечивает доступ к настройкам для всех компонентов пайплайна.
    """

    def __init__(self, config_file="config/default_settings.json"):
        self.config_file = config_file
        self._config = {}
        # Конфигурация по умолчанию (Hardcoded)
        self._defaults = {
            'database': {
                'host': 'localhost',
                'port': '5432',
                'database': 'postgres',
                'user': 'postgres',
                'password': '',
                'default_table': 'imported_data'
            },
            'file_settings': {
                'max_file_size_mb': 100,
                'allowed_extensions': ['.csv', '.xlsx', '.xls']
            },
            'smtp': {
                'server': 'smtp.mail.ru',
                'port': 587,
                'user': 'nikitkov.vasya@mail.ru',
                'password': '2kSIW37RjYAw2CK38OhM',
                'use_tls': True
            },
            'app': {
                'report_dir': 'reports',
                'log_level': 'INFO'
            }
        }

        # Загружаем конфигурацию
        self._config = self.load_config()

    def load_config(self) -> dict:
        """
        Загружает конфигурацию из файла.
        Объединяет загруженные данные с дефолтными значениями.
        """
        config = self._defaults.copy()

        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                # Глубокое обновление секций
                for section, values in loaded_data.items():
                    if section in config and isinstance(values, dict):
                        config[section].update(values)
                    else:
                        config[section] = values
                logging.info(f"Конфигурация успешно загружена из {self.config_file}")
            except Exception as e:
                logging.error(f"Ошибка при чтении файла конфигурации: {e}")
        else:
            logging.info("Файл конфигурации не найден. Используются значения по умолчанию.")
            self.save_config(config)  # Создаём файл, если его нет

        return config

    def save_config(self, config: dict | None = None):
        """
        Сохраняет текущую конфигурацию в JSON файл.
        """
        data_to_save = config if config is not None else self._config
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=4, ensure_ascii=False)
            logging.info(f"Конфигурация сохранена в {self.config_file}")
        except Exception as e:
            logging.error(f"Ошибка при сохранении конфигурации: {e}")

    def get(self, section: str, default: Any = None) -> Any:
        """
        Безопасно возвращает секцию или значение из конфигурации.
        Пример: config.get('database')
        """
        return self._config.get(section, default)

    def set(self, section: str, values: dict):
        """
        Устанавливает или обновляет секцию конфигурации в памяти.
        Для записи на диск нужно вызвать save_config().
        """
        if section not in self._config:
            self._config[section] = {}

        if isinstance(values, dict):
            self._config[section].update(values)
        else:
            self._config[section] = values

        logging.debug(f"Секция {section} обновлена в памяти.")

    def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Безопасное получение настройки с значением по умолчанию.
        Поддерживает доступ через точку: 'database.host'
        """
        keys = key.split('.')
        current = self._config

        try:
            for k in keys:
                current = current[k]
            return current
        except (KeyError, TypeError):
            return default

    @property
    def all_settings(self) -> dict:
        """Возвращает весь словарь настроек."""
        return self._config

    def get_default_config(self) -> dict:
        """Возвращает чистую конфигурацию по умолчанию."""
        return self._defaults.copy()