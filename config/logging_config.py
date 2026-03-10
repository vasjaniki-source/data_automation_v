import os
from .settings import LOGS_DIR

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
        "detailed": {
            "format": "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
        },
        "app_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "app.log"),
            "maxBytes": 5242880,  # 5MB
            "backupCount": 3,
            "formatter": "detailed",
            "encoding": "utf-8",
        },
        "gui_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "gui_events.log"),
            "maxBytes": 2097152,  # 2MB
            "backupCount": 2,
            "formatter": "standard",
            "encoding": "utf-8",
        },
    },
    "loggers": {
        "": {  # Root logger
            "handlers": ["console", "app_file"],
            "level": "DEBUG",
            "propagate": True
        },
        "gui": {
            "handlers": ["console", "gui_file"],
            "level": "INFO",
            "propagate": False
        },
    }
}