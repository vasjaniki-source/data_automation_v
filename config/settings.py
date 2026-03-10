import os
from pathlib import Path

# Базовая директория проекта
BASE_DIR = Path(__file__).resolve().parent.parent

# Пути к данным и отчетам
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
MODELS_DIR = os.path.join(BASE_DIR, "models", "trained")

# Создание необходимых директорий
for folder in [REPORTS_DIR, LOGS_DIR, MODELS_DIR]:
    os.makedirs(folder, exist_ok=True)

# Конфигурация по умолчанию
DEFAULT_CONFIG_PATH = os.path.join(BASE_DIR, "config", "default_settings.json")