from logging.handlers import RotatingFileHandler
import os
import tkinter as tk
from tkinter import ttk, scrolledtext
import logging
import threading
import traceback

from typing import Literal, Callable, Optional, Any, cast
from pathlib import Path

from sklearn.pipeline import Pipeline

from gui.widgets.db_config import DBConfigWidget
from gui.widgets.file_selector import FileSelectorWidget
from gui.widgets.history_panel import HistoryPanelWidget
from gui.widgets.settings_panel import SettingsPanelWidget
from gui.widgets.visualization_panel import VisualizationPanelWidget

# Попытка импортировать реальные зависимости. Если их нет — бросаем ImportError с понятной подсказкой.
try:
    from utils.config_manager import ConfigManager  # type: ignore
except Exception as e:
    raise ImportError(
        "Не найден модуль utils.config_manager с классом ConfigManager. "
        "Добавьте модуль или исправьте путь импорта."
    ) from e

try:
    from data_processor.pipeline import DataProcessingPipeline  # type: ignore
except Exception as e:
    raise ImportError(
        "Не найден модуль data_processor.pipeline с классом DataProcessingPipeline. "
        "Добавьте модуль или исправьте путь импорта."
    ) from e


# --- fallback logger: используется при ошибках внутри GUI-логики, НЕ пишет в GUI ---
fallback_logger = logging.getLogger("fallback_no_gui")
if not fallback_logger.handlers:
    fh = logging.StreamHandler()
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    fallback_logger.addHandler(fh)
fallback_logger.setLevel(logging.INFO)


# --- Thread-local flag to prevent re-entrant emits in TkinterLogHandler ---
_emit_local = threading.local()


class TkinterLogHandler(logging.Handler):
    """
    Logging.Handler, безопасно добавляющий записи в scrolledtext.
    Защита от реентрантного emit (чтобы не было рекурсивных вызовов).
    """
    def __init__(self, text_widget: scrolledtext.ScrolledText):
        super().__init__()
        self.text_widget = text_widget
        self.setFormatter(logging.Formatter('%H:%M:%S %(levelname)s: %(message)s'))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if getattr(_emit_local, "in_emit", False):
                try:
                    fallback_logger.log(record.levelno, "Re-entrant emit suppressed: %s", self.format(record))
                except Exception:
                    pass
                return

            msg = self.format(record)

            def append():
                try:
                    _emit_local.in_emit = True
                    if not self.text_widget:
                        return

                    self.text_widget.config(state="normal")
                    self.text_widget.insert(tk.END, msg + "\n")

                    try:
                        line_index = int(self.text_widget.index("end-1c").split('.')[0])
                        start = f"{line_index}.0"
                        end = f"{line_index}.end"
                    except Exception:
                        start = "end-2l"
                        end = "end-1l"

                    if record.levelno >= logging.ERROR:
                        self.text_widget.tag_add("error", start, end)
                        self.text_widget.tag_config("error", foreground="red")
                    elif record.levelno >= logging.WARNING:
                        self.text_widget.tag_add("warning", start, end)
                        self.text_widget.tag_config("warning", foreground="orange")
                    else:
                        self.text_widget.tag_add("info", start, end)
                        self.text_widget.tag_config("info", foreground="black")

                    self.text_widget.config(state="disabled")
                    self.text_widget.see(tk.END)
                except Exception as e:
                    fallback_logger.error("Exception in TkinterLogHandler.append: %s\n%s", e, traceback.format_exc())
                finally:
                    try:
                        _emit_local.in_emit = False
                    except Exception:
                        pass

            try:
                if self.text_widget:
                    self.text_widget.after(0, append)
                else:
                    fallback_logger.log(record.levelno, msg)
            except Exception as e:
                fallback_logger.error("Could not schedule append in TkinterLogHandler: %s\n%s", e, traceback.format_exc())

        except Exception as ex:
            try:
                fallback_logger.error("Unexpected error in TkinterLogHandler.emit: %s\n%s", ex, traceback.format_exc())
            except Exception:
                pass


class DataAutomationGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Система автоматизации данных v5.1 (Improved)")
        self.root.geometry("1150x900")

        # Thread-local state for add_to_history recursion guard
        self._local = threading.local()
        self._local.add_history_depth = 0

        # Attributes
        self.pipeline: Optional[DataProcessingPipeline] = None
        self.config_manager: Optional[ConfigManager] = None
        self.log_text: Optional[scrolledtext.ScrolledText] = None
        self.history_tab: Optional[Any] = None

        # Инициализация конфигурации и пайплайна — если тут возникнут ошибки, бросаем исключение,
        # чтобы разработчик знал, что отсутствуют реальные реализации.
        config_path = Path("config/default_settings.json")
        if not config_path.exists():
            fallback_logger.info("Config file not found; creating default empty config.")
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text("{}", encoding="utf-8")

        # Создаём реальные экземпляры (предполагается, что импорты выше успешны)
        try:
            self.config_manager = ConfigManager(str(config_path))
        except Exception as e:
            # Явно информируем, что ConfigManager не инициализирован корректно
            raise RuntimeError(f"Не удалось инициализировать ConfigManager: {e}") from e

        try:
            self.pipeline = DataProcessingPipeline(self.config_manager)
        except Exception as e:
            raise RuntimeError(f"Не удалось инициализировать DataProcessingPipeline: {e}") from e

        # Создание макета и виджетов
        self.create_layout()

        # Подключаем обработчик логов к 'gui_log'
        self.setup_logging()

        # Обновляем статус
        if self.pipeline:
            self.set_status("Система готова")
            logging.getLogger("gui").info("Состояние: Система готова к работе.")
        else:
            self.set_status("Система: Ошибка инициализации, функционал ограничен")
            logging.getLogger("gui").warning("Состояние: Ошибка инициализации. Некоторые функции могут быть недоступны.")

    def _create_log_callback(self) -> Callable[[str, str], None]:
        logger = logging.getLogger("gui_log")

        def log_callback(message: str, level: str = "INFO") -> None:
            if not message:
                return
            lvl_name = (level or "INFO").upper()
            if lvl_name not in ("INFO", "WARNING", "ERROR", "DEBUG"):
                lvl_name = "INFO"
            try:
                if lvl_name == "ERROR":
                    logger.error(message)
                elif lvl_name == "WARNING":
                    logger.warning(message)
                elif lvl_name == "DEBUG":
                    logger.debug(message)
                else:
                    logger.info(message)
            except Exception as e:
                fallback_logger.error("log_callback failed to log message: %s | %s\n%s", message, e, traceback.format_exc())

        return log_callback

    def is_file_locked(self, filepath: str) -> bool:
        try:
            if not os.path.exists(filepath):
                return False
            with open(filepath, "a", encoding="utf-8"):
                pass
            return False
        except (OSError, PermissionError):
            return True

    def setup_logging(self) -> None:
        log_path = os.path.join("logs", "app.log")
        try:
            os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        except Exception as e:
            fallback_logger.error("Could not create log directory: %s\n%s", e, traceback.format_exc())

        if self.is_file_locked(log_path):
            fallback_logger.critical("Log file is locked or inaccessible: %s", log_path)

        gui_log_logger = logging.getLogger("gui_log")
        for h in list(gui_log_logger.handlers):
            try:
                gui_log_logger.removeHandler(h)
                h.close()
            except Exception:
                pass

        try:
            file_handler = RotatingFileHandler(
                log_path,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8',
                delay=True
            )
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            gui_log_logger.addHandler(file_handler)
            fallback_logger.info("File handler attached to 'gui_log'.")
        except Exception as e:
            fallback_logger.error("Cannot attach file handler: %s\n%s", e, traceback.format_exc())

        if self.log_text:
            try:
                gui_handler = TkinterLogHandler(self.log_text)
                gui_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
                gui_log_logger.addHandler(gui_handler)
                fallback_logger.info("TkinterLogHandler attached to 'gui_log'.")
            except Exception as e:
                fallback_logger.error("Failed to attach TkinterLogHandler: %s\n%s", e, traceback.format_exc())
        else:
            fallback_logger.warning("log_text widget not ready; TkinterLogHandler not attached.")

        gui_log_logger.setLevel(logging.INFO)
        gui_log_logger.propagate = False

        fallback_logger.info("Logging setup completed.")

    def add_to_history(self, msg: str, status: Literal["INFO", "WARNING", "ERROR"] = "INFO") -> None:
        depth = getattr(self._local, "add_history_depth", 0)
        if depth > 0:
            fallback_logger.warning("Suppressed re-entrant add_to_history call. Message: %s", msg)
            return

        self._local.add_history_depth = depth + 1
        try:
            if self.history_tab is None:
                fallback_logger.info("History widget is None. Message: %s", msg)
                return

            add_entry_func = getattr(self.history_tab, "add_entry", None)
            if not callable(add_entry_func):
                fallback_logger.info("history_tab has no callable add_entry. Message: %s", msg)
                return

            def do_add():
                try:
                    title = msg.split(":", 1)[0] if ":" in msg else "Log"
                    add_entry_func(title, status, msg)
                except Exception as ex:
                    fallback_logger.error("history_tab.add_entry failed: %s\n%s", ex, traceback.format_exc())

            if threading.current_thread() is threading.main_thread():
                do_add()
            else:
                try:
                    self.root.after(0, do_add)
                except Exception as fallback_ex:
                    fallback_logger.warning("Could not schedule history add via after(): %s. Calling directly.", fallback_ex)
                    try:
                        do_add()
                    except Exception as ex2:
                        fallback_logger.error("Direct call to do_add failed: %s\n%s", ex2, traceback.format_exc())
        finally:
            try:
                self._local.add_history_depth -= 1
            except Exception:
                self._local.add_history_depth = 0

    def create_layout(self) -> None:
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=5)

        log_frame = ttk.LabelFrame(self.root, text="Системный журнал (Real-time)", padding="5")
        log_frame.pack(fill="x", padx=10, pady=5)
        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=8, state="disabled",
            font=("Consolas", 9), bg="#f8f9fa", wrap=tk.WORD
        )
        self.log_text.pack(fill="x", side="left", expand=True)

        self.status_var = tk.StringVar(value="Инициализация...")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W, padding=(5, 2))
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        log_callback: Callable[[str, str], None] = self._create_log_callback()

        # Прямые вызовы реальных виджетов. Если инициализация виджета вызывает исключение,
        # оно логируется, и соответствующее поле остаётся None.
        try:
            pipeline_typed = cast(Pipeline, self.pipeline)
            self.history_tab = HistoryPanelWidget(self.notebook, pipeline_typed, log_callback)
            self.notebook.add(self.history_tab, text="📜 История")
        except Exception as e:
            fallback_logger.error("Failed to init HistoryPanelWidget: %s\n%s", e, traceback.format_exc())
            self.history_tab = None

        try:
            self.load_tab = FileSelectorWidget(self.notebook, self.pipeline, log_callback)
            self.notebook.add(self.load_tab, text="📥 Загрузка и Отчеты")
        except Exception as e:
            fallback_logger.error("Failed to init FileSelectorWidget: %s\n%s", e, traceback.format_exc())
            self.load_tab = None

        try:
            self.db_tab = DBConfigWidget(self.notebook, self.pipeline, log_callback)
            self.notebook.add(self.db_tab, text="🗄️ База данных")
        except Exception as e:
            fallback_logger.error("Failed to init DBConfigWidget: %s\n%s", e, traceback.format_exc())
            self.db_tab = None

        try:
            self.viz_tab = VisualizationPanelWidget(self.notebook, self.pipeline, log_callback)
            self.notebook.add(self.viz_tab, text="📊 Визуализация")
        except Exception as e:
            fallback_logger.error("Failed to init VisualizationPanelWidget: %s\n%s", e, traceback.format_exc())
            self.viz_tab = None

        try:
            self.settings_tab = SettingsPanelWidget(self.notebook, self.pipeline, log_callback)
            self.notebook.add(self.settings_tab, text="⚙️ Настройки")
        except Exception as e:
            fallback_logger.error("Failed to init SettingsPanelWidget: %s\n%s", e, traceback.format_exc())
            self.settings_tab = None

    def set_status(self, text: str) -> None:
        try:
            self.status_var.set(text)
        except Exception:
            fallback_logger.error("Failed to set status: %s", traceback.format_exc())


# --- Пример запуска приложения (самостоятельный модуль) ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    root = tk.Tk()
    try:
        style = ttk.Style()
        available_themes = style.theme_names()
        chosen_theme = "clam" if "clam" in available_themes else ("alt" if "alt" in available_themes else "default")
        style.theme_use(chosen_theme)
        fallback_logger.info("Theme used: %s", chosen_theme)
    except Exception:
        fallback_logger.warning("Could not set ttk theme; using default.")

    app = DataAutomationGUI(root)
    root.mainloop()
