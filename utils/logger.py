
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Tuple, Optional, Any, Callable # Added Any, Callable for callback type hinting

def setup_app_logging(
    log_level: str = "INFO",
    log_dir: str = "logs",
    max_bytes: int = 10**6,
    backup_count: int = 5,
    console: bool = True,
    force: bool = False,
    gui_log_filename: str = "gui_events.log",
    app_log_filename: str = "app.log",
    console_handler_level: Optional[str] = None, # Allow different level for console
    file_handler_level: Optional[str] = None # Allow different level for files
) -> logging.Logger:
    """
    Настройка глобального логирования для всего проекта.

    Args:
        log_level: Основной уровень логирования (строка, например "INFO" или "DEBUG", либо числовой код).
        log_dir: директория для лог-файлов.
        max_bytes: maxBytes для RotatingFileHandler.
        backup_count: backupCount для RotatingFileHandler.
        console: добавлять ли консольный хендлер.
        force: если True — пересоздать хендлеры, даже если уже настроены.
        gui_log_filename: имя файла для GUI событий.
        app_log_filename: имя файла для основного лога приложения.
        console_handler_level: специфический уровень для консоли (переопределяет log_level).
        file_handler_level: специфический уровень для файловых логов (переопределяет log_level).

    Возвращает:
        root logger.
    """
    # --- 1. Разбор уровней логирования ---
    def _parse_level(level_str: str, default_level: int = logging.INFO) -> int:
        """Разбирает строковое представление уровня логирования."""
        if isinstance(level_str, int):
            return level_str
        if not isinstance(level_str, str):
            return default_level
        
        level_str = level_str.upper()
        level = getattr(logging, level_str, None)
        if level is None:
            try:
                level = int(level_str)
            except ValueError:
                level = default_level
        return level

    main_level = _parse_level(log_level)
    console_level = _parse_level(console_handler_level, main_level) if console_handler_level else main_level
    file_level = _parse_level(file_handler_level, main_level) if file_handler_level else main_level

    # --- 2. Форматтер ---
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # --- 3. Получение корневого логгера ---
    root_logger = logging.getLogger()
    # Установка основного уровня для корневого логгера.
    # Уровни хендлеров будут установлены ниже.
    root_logger.setLevel(main_level) 

    # --- 4. Обработка существующих хендлеров ---
    # Используем `force` флаг для полного сброса или проверяем наличие нужных хендлеров
    if not force:
        has_app_log = False
        has_gui_log = False
        has_console = False
        
        for handler in root_logger.handlers:
            if isinstance(handler, RotatingFileHandler):
                if handler.baseFilename.endswith(app_log_filename):
                    has_app_log = True
                elif handler.baseFilename.endswith(gui_log_filename):
                    has_gui_log = True
            elif isinstance(handler, logging.StreamHandler):
                has_console = True

        # Если всё уже настроено, обновляем уровни и форматтеры, но не добавляем новые хендлеры
        if has_app_log and has_gui_log and (not console or has_console):
            for handler in root_logger.handlers:
                handler.setLevel(main_level) # Обновляем уровень
                handler.setFormatter(formatter) # Обновляем форматтер
            # Обновляем уровень для gui_logger, если он существует
            if logging.getLogger("gui").level != main_level:
                 logging.getLogger("gui").setLevel(main_level)
            return root_logger
    else:
        # Если force=True, удаляем все старые хендлеры
        for handler in list(root_logger.handlers):
            try:
                root_logger.removeHandler(handler)
                handler.close() # Важно для закрытия файловых дескрипторов
            except Exception as e:
                # Логируем ошибку при удалении, но не останавливаем процесс
                print(f"Error closing existing handler {handler}: {e}", file=sys.stderr) # Fallback logging
                logging.getLogger(__name__).error(f"Error closing existing handler {handler}: {e}", exc_info=True)

    # --- 5. Создание и настройка хендлеров ---
    try:
        os.makedirs(log_dir, exist_ok=True) # Безопасное создание директории
    except OSError as e:
        # Если не можем создать директорию, логируем ошибку в консоль (если она включена)
        print(f"FATAL: Could not create log directory '{log_dir}'. Permission denied or invalid path: {e}", file=sys.stderr)
        # Если консоль не была настроена, это будет единственное сообщение об ошибке
        if not console:
             # Попытка логировать в fallback
             try:
                 fallback_logger = logging.getLogger("fallback_error")
                 fallback_logger.setLevel(logging.ERROR)
                 fallback_handler = logging.StreamHandler(sys.stderr)
                 fallback_handler.setFormatter(formatter)
                 fallback_logger.addHandler(fallback_handler)
                 fallback_logger.error(f"FATAL: Could not create log directory '{log_dir}'. Permission denied or invalid path: {e}", exc_info=True)
             except Exception:
                 pass # Если даже fallback не работает, ничего не можем сделать
        raise # Прекращаем выполнение, если лог-директорию создать не удалось

    # --- Файловые хендлеры ---
    for filename, handler_level in [(app_log_filename, file_level), (gui_log_filename, file_level)]:
        log_path = os.path.join(log_dir, filename)
        try:
            # RotatingFileHandler может вызвать PermissionError, если нет прав на запись
            handler = RotatingFileHandler(
                log_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8" # Указываем кодировку
            )
            handler.setLevel(handler_level) # Устанавливаем специфичный уровень для файла
            handler.setFormatter(formatter)
            
            # Добавляем handler к root_logger (для app.log) или gui_logger (для gui_events.log)
            if filename == app_log_filename:
                root_logger.addHandler(handler)
            elif filename == gui_log_filename:
                gui_logger = logging.getLogger("gui")
                # Предотвращаем дублирование логов GUI
                gui_logger.propagate = False
                gui_logger.setLevel(handler_level)
                
                # Проверяем, есть ли уже handler для gui_events.log
                gui_handler_exists = any(
                    isinstance(h, RotatingFileHandler) and h.baseFilename.endswith(filename)
                    for h in gui_logger.handlers
                )
                if not gui_handler_exists:
                    gui_logger.addHandler(handler)
                else:
                    # Если handler уже есть, просто обновляем его уровень и форматтер
                    for h in gui_logger.handlers:
                        if isinstance(h, RotatingFileHandler) and h.baseFilename.endswith(filename):
                            h.setLevel(handler_level)
                            h.setFormatter(formatter)

        except PermissionError:
            error_msg = f"PermissionError: Не удалось создать или записать в лог-файл '{log_path}'. Проверьте права доступа."
            print(f"ERROR: {error_msg}", file=sys.stderr) # Fallback logging
            logging.getLogger("fallback_error").error(error_msg, exc_info=True) # Try logging to a fallback if possible
            # Продолжаем, но без этого файлового логгера
        except Exception as e:
            error_msg = f"Error setting up RotatingFileHandler for '{log_path}': {e}"
            print(f"ERROR: {error_msg}", file=sys.stderr) # Fallback logging
            logging.getLogger("fallback_error").error(error_msg, exc_info=True)
            # Продолжаем

    # --- Консольный хендлер ---
    if console:
        try:
            console_handler = logging.StreamHandler(stream=sys.stdout)
            console_handler.setLevel(console_level) # Используем специфичный уровень для консоли
            console_handler.setFormatter(formatter)
            
            # Проверяем, есть ли уже console handler
            console_handler_exists = any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers)
            if not console_handler_exists:
                root_logger.addHandler(console_handler)
            else:
                # Если есть, обновляем уровень и форматтер
                for h in root_logger.handlers:
                    if isinstance(h, logging.StreamHandler):
                        h.setLevel(console_level)
                        h.setFormatter(formatter)
        except Exception as e:
            error_msg = f"Error setting up console handler: {e}"
            print(f"ERROR: {error_msg}", file=sys.stderr)
            logging.getLogger("fallback_error").error(error_msg, exc_info=True)

    # --- Финальная проверка ---
    # Убедимся, что корневой логгер имеет хотя бы один хендлер, иначе логи не будут выводиться
    if not root_logger.handlers:
        print("WARNING: No logging handlers were successfully configured. Logging might not work.", file=sys.stderr)
        # Можно добавить fallback StreamHandler, если консоль была выключена
        if not console:
            try:
                fallback_handler = logging.StreamHandler(sys.stderr)
                fallback_handler.setFormatter(formatter)
                fallback_handler.setLevel(main_level)
                root_logger.addHandler(fallback_handler)
                print("INFO: Added fallback StreamHandler to stderr.", file=sys.stderr)
            except Exception:
                pass # Если даже это не работает

    return root_logger

# --- Пример использования ---
if __name__ == "__main__":
    # 1. Базовая настройка (INFO уровень, консоль включена)
    print("--- Настройка 1: Базовая ---")
    root_logger = setup_app_logging(log_level="INFO", force=True)
    logging.info("Основное сообщение логирования (INFO).")
    logging.debug("Это сообщение DEBUG, оно не должно появиться при INFO уровне.")
    logging.warning("Это сообщение WARNING.")

    # 2. Настройка с DEBUG уровнем и отключенной консолью
    print("\n--- Настройка 2: DEBUG уровень, консоль отключена ---")
    root_logger_debug = setup_app_logging(log_level="DEBUG", console=False, force=True)
    logging.info("Сообщение INFO при DEBUG уровне.")
    logging.debug("Сообщение DEBUG при DEBUG уровне (должно появиться).")
    logging.error("Сообщение ERROR.")

    # 3. Настройка с разными уровнями для файлов и консоли
    print("\n--- Настройка 3: Разные уровни ---")
    root_logger_mixed = setup_app_logging(
        log_level="INFO", # Основной уровень
        console_handler_level="DEBUG", # Консоль покажет DEBUG
        file_handler_level="WARNING",  # Файлы будут писать только WARNING и выше
        force=True
    )
    logging.info("INFO сообщение.") # Не попадет в файлы, но увидим в консоли
    logging.debug("DEBUG сообщение.") # Не попадет никуда, кроме консоли
    logging.warning("WARNING сообщение.") # Попадет везде
    logging.error("ERROR сообщение.")   # Попадет везде

    # 4. Пример с PermissionError (если папка 'logs' не доступна для записи)
    print("\n--- Настройка 4: Имитация PermissionError ---")
    # Здесь мы не можем реально вызвать PermissionError без изменения прав системы,
    # но мы можем увидеть, как код обрабатывает ошибки при настройке хендлеров.
    # Если бы 'logs' была недоступна, мы бы увидели сообщения в stderr.
    # Например, попробуйте передать несуществующий путь или убрать права на запись:
    # setup_app_logging(log_dir="/root/nonexistent_logs", force=True) # Пример, который вызовет ошибку
    
    # 5. Пример с GUI логгером
    print("\n--- Настройка 5: GUI логгер ---")
    gui_logger = logging.getLogger("gui")
    gui_logger.info("Это сообщение для GUI логгера.")
    gui_logger.warning("Еще одно сообщение для GUI.")
    # Это сообщение не должно попасть в app.log из-за propagate=False
    # Оно должно попасть только в gui_events.log (если настроен)
    logging.info("Это обычное сообщение приложения.")

    print("\n--- Завершение настройки логирования ---")

    logging.shutdown()
