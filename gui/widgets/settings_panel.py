import tkinter as tk
from tkinter import Toplevel, StringVar, BooleanVar, Entry, Label, Button, Frame, Checkbutton, LabelFrame, messagebox
from tkinter import ttk, filedialog
import logging
from pathlib import Path
from typing import Any, Optional, Callable, Protocol, cast

# Попытка импортировать реальный ConfigManager; если его нет — используем локальную заглушку
try:
    from utils.config_manager import ConfigManager  # type: ignore
except Exception:
    ConfigManager = None  # позднее будет заменён на MockConfigManager

logger = logging.getLogger(__name__)


class IConfigManager(Protocol):
    def get(self, key: str, default: Any = None) -> Any: ...
    def get_setting(self, path: str, default: Any = None) -> Any: ...
    def set_setting(self, path: str, value: Any) -> None: ...
    def save_config(self) -> None: ...
    def delete(self, path: str) -> None: ...


# Mock-реализация ConfigManager, если реальный недоступен
class MockConfigManager:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        logger.warning("MockConfigManager используется, функциональность сохранения/загрузки настроек ограничена.")
        self._settings: dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._settings.get(key, default)

    def get_setting(self, path: str, default: Any = None) -> Any:
        if not path:
            return default
        keys = path.split('.')
        current: Any = self._settings
        for k in keys:
            if isinstance(current, dict):
                current = current.get(k, None)
            else:
                return default
        return current if current is not None else default

    def set_setting(self, path: str, value: Any) -> None:
        if not path:
            return
        keys = path.split('.')
        current = self._settings
        for i, k in enumerate(keys):
            if i == len(keys) - 1:
                current[k] = value
            else:
                if k not in current or not isinstance(current[k], dict):
                    current[k] = {}
                current = current[k]

    def save_config(self) -> None:
        logger.info("MockConfigManager: Настройки не сохраняются в файл (mock).")

    def delete(self, path: str) -> None:
        # Простая реализация удаления, если путь указывает на ключ верхнего уровня
        if not path:
            return
        keys = path.split('.')
        current = self._settings
        for i, k in enumerate(keys):
            if i == len(keys) - 1:
                if isinstance(current, dict) and k in current:
                    del current[k]
                return
            current = current.get(k, {})
            if not isinstance(current, dict):
                return


# Заглушка для Pipeline (если реальная недоступна)
class MockPipeline:
    def __init__(self) -> None:
        self.config_manager = MockConfigManager()
        self.output_dir = Path.cwd()


# Если реального ConfigManager не импортировали, используем MockConfigManager
if ConfigManager is None:
    ConfigManager = MockConfigManager  # type: ignore


class SettingsPanelWidget(Frame):
    def __init__(
        self,
        parent: tk.Misc,
        pipeline: Optional[Any],
        log_callback: Optional[Callable[[str, str], None]] = None
    ) -> None:
        super().__init__(parent, padx=20, pady=20)
        self.pipeline = pipeline

        # Пытаемся получить config_manager из pipeline, иначе используем MockConfigManager
        cfg = None
        if pipeline is not None:
            cfg = getattr(pipeline, 'config_manager', None)

        if cfg is None:
            # Проверяем, что ConfigManager не None перед вызовом
            if ConfigManager is None:
                # fallback на MockConfigManager, если ConfigManager недоступен
                self.config_manager: IConfigManager = cast(IConfigManager, MockConfigManager())
            else:
                self.config_manager: IConfigManager = cast(IConfigManager, ConfigManager())
        else:
            self.config_manager = cfg

        # Безопасный log_callback — если не передан, используем локальный logger
        if callable(log_callback):
            self.log_callback = log_callback
        else:
            def _local_log(msg: str, level: str = "INFO") -> None:
                level = (level or "INFO").upper()
                if level == "ERROR":
                    logger.error(msg)
                elif level == "WARNING":
                    logger.warning(msg)
                else:
                    logger.info(msg)
            self.log_callback = _local_log

        # Переменные для хранения текущих настроек
        self.report_path_var = StringVar()
        self.send_email_var = BooleanVar()
        self.email_recipients_var = StringVar()

        # Новые переменные для SMTP
        self.smtp_server_var = StringVar()
        self.smtp_port_var = StringVar()
        self.smtp_user_var = StringVar()
        self.smtp_password_var = StringVar()  # Пароль не должен загружаться автоматически
        self.smtp_use_tls_var = BooleanVar()

        self.create_widgets()
        self.load_settings()

    def create_widgets(self) -> None:
        # Раздел "Настройки отчётов"
        path_frame = LabelFrame(self, text="Настройки отчётов", padx=10, pady=10)
        path_frame.pack(fill="x", pady=10)

        entry_btn_frame = Frame(path_frame)
        entry_btn_frame.pack(fill="x")

        Label(entry_btn_frame, text="Путь сохранения:").pack(anchor="w", side="left")
        self.report_path_entry = Entry(entry_btn_frame, textvariable=self.report_path_var, width=40)
        self.report_path_entry.pack(anchor="w", side="left", fill="x", expand=True, pady=5, padx=(5, 0))

        Button(entry_btn_frame, text="Обзор...", command=self.browse_report_path).pack(anchor="w", side="left", padx=(5, 0))

        # Раздел "Отправка по Email"
        email_frame = LabelFrame(self, text="Отправка по Email", padx=10, pady=10)
        email_frame.pack(fill="x", pady=10)

        self.send_email_check = Checkbutton(
            email_frame,
            text="Отправлять отчёт по email",
            variable=self.send_email_var,
            command=self.toggle_email_fields
        )
        self.send_email_check.pack(anchor="w", pady=5)

        # Контейнер для полей email
        self.email_recipients_container = Frame(email_frame)
        self.email_recipients_container.pack(fill="x")
        self.email_recipients_label = Label(self.email_recipients_container, text="Email получателей (через запятую):")
        self.email_recipients_label.pack(anchor="w")
        self.email_recipients_entry = Entry(self.email_recipients_container, textvariable=self.email_recipients_var, width=40)
        self.email_recipients_entry.pack(fill="x", pady=5)

        # Раздел "Параметры SMTP сервера"
        self.smtp_frame = LabelFrame(email_frame, text="Настройки SMTP сервера", padx=10, pady=10)
        self.smtp_frame.pack(fill="x", pady=5)

        smtp_server_row = Frame(self.smtp_frame)
        smtp_server_row.pack(fill="x")
        Label(smtp_server_row, text="SMTP сервер:").pack(side="left", anchor="w")
        self.smtp_server_entry = Entry(smtp_server_row, textvariable=self.smtp_server_var, width=30)
        self.smtp_server_entry.pack(side="left", fill="x", expand=True, padx=5)

        smtp_port_row = Frame(self.smtp_frame)
        smtp_port_row.pack(fill="x", pady=5)
        Label(smtp_port_row, text="Порт:").pack(side="left", anchor="w")
        self.smtp_port_entry = Entry(smtp_port_row, textvariable=self.smtp_port_var, width=10)
        self.smtp_port_entry.pack(side="left", padx=5)

        smtp_user_row = Frame(self.smtp_frame)
        smtp_user_row.pack(fill="x")
        Label(smtp_user_row, text="Логин (Email):").pack(side="left", anchor="w")
        self.smtp_user_entry = Entry(smtp_user_row, textvariable=self.smtp_user_var, width=30)
        self.smtp_user_entry.pack(side="left", fill="x", expand=True, padx=5)

        smtp_password_row = Frame(self.smtp_frame)
        smtp_password_row.pack(fill="x", pady=5)
        Label(smtp_password_row, text="Пароль:").pack(side="left", anchor="w")
        # Пароль не загружается из конфига. Он только вводится пользователем.
        self.smtp_password_entry = Entry(smtp_password_row, textvariable=self.smtp_password_var, show="*", width=30)
        self.smtp_password_entry.pack(side="left", fill="x", expand=True, padx=5)

        self.smtp_use_tls_check = Checkbutton(self.smtp_frame, text="Использовать TLS/STARTTLS", variable=self.smtp_use_tls_var)
        self.smtp_use_tls_check.pack(anchor="w", pady=5)

        # Изначально скрываем email и SMTP поля (toggle_email_fields покажет/скроет)
        self.toggle_email_fields()

        # Кнопки
        btn_frame = Frame(self)
        btn_frame.pack(pady=20)
        Button(btn_frame, text="Применить", command=self.apply_settings).pack(side="left", padx=5)
        Button(btn_frame, text="Сброс", command=self.reset_settings).pack(side="left", padx=5)

    def browse_report_path(self) -> None:
        """Открывает диалог выбора директории для сохранения отчётов."""
        initial_dir = self.report_path_var.get()
        if not initial_dir or not Path(initial_dir).is_dir():
            initial_dir = str(Path.home())
        directory = filedialog.askdirectory(initialdir=initial_dir, title="Выберите папку для сохранения отчётов")
        if directory:
            self.report_path_var.set(directory)
            logger.debug("Выбрана новая директория для отчётов: %s", directory)
            self.log_callback(f"Выбрана директория для отчётов: {directory}", "INFO")

    def toggle_email_fields(self) -> None:
        """Показывает/скрывает поля для ввода email и SMTP."""
        if self.send_email_var.get():
            self.email_recipients_container.pack(fill="x", pady=(5, 0), padx=10)
            self.smtp_frame.pack(fill="x", pady=5, padx=10)
        else:
            self.email_recipients_container.pack_forget()
            self.smtp_frame.pack_forget()

    def load_settings(self) -> None:
        """Загружает настройки из config_manager и обновляет виджеты."""
        self.log_callback("Загрузка настроек панели...", "INFO")
        try:
            # Путь сохранения отчётов
            report_dir_cfg = None
            try:
                report_dir_cfg = self.config_manager.get_setting('app.report_dir')
            except Exception:
                logger.debug("config_manager.get_setting('app.report_dir') вызвала исключение.", exc_info=True)

            if isinstance(report_dir_cfg, str) and report_dir_cfg:
                self.report_path_var.set(report_dir_cfg)
            else:
                pipeline_output_dir = getattr(self.pipeline, 'output_dir', None)
                if isinstance(pipeline_output_dir, Path):
                    self.report_path_var.set(str(pipeline_output_dir))
                else:
                    self.report_path_var.set(str(Path.cwd()))

            # Настройки Email
            send_email_cfg = None
            try:
                send_email_cfg = self.config_manager.get_setting('email.send')
            except Exception:
                logger.debug("config_manager.get_setting('email.send') вызвала исключение.", exc_info=True)

            self.send_email_var.set(bool(send_email_cfg) if send_email_cfg is not None else False)

            recipients_cfg = None
            try:
                recipients_cfg = self.config_manager.get_setting('email.recipients')
            except Exception:
                logger.debug("config_manager.get_setting('email.recipients') вызвала исключение.", exc_info=True)

            if isinstance(recipients_cfg, str):
                self.email_recipients_var.set(recipients_cfg)
            else:
                self.email_recipients_var.set('')

            # Настройки SMTP
            smtp_server_cfg = self.config_manager.get_setting('smtp.host')
            self.smtp_server_var.set(smtp_server_cfg if isinstance(smtp_server_cfg, str) else '')

            smtp_port_cfg = self.config_manager.get_setting('smtp.port')
            self.smtp_port_var.set(str(smtp_port_cfg) if isinstance(smtp_port_cfg, int) else '')

            smtp_user_cfg = self.config_manager.get_setting('smtp.user')
            self.smtp_user_var.set(smtp_user_cfg if isinstance(smtp_user_cfg, str) else '')

            # Пароль — не загружаем в поле Entry по соображениям безопасности
            self.smtp_password_var.set('')

            use_tls_cfg = self.config_manager.get_setting('smtp.use_tls')
            self.smtp_use_tls_var.set(bool(use_tls_cfg) if use_tls_cfg is not None else False)

            # Обновляем видимость полей email и SMTP
            self.toggle_email_fields()

            self.log_callback("Настройки отчётов, email и SMTP успешно загружены.", "INFO")

        except Exception as e:
            logger.exception("Ошибка при загрузке настроек")
            messagebox.showerror("Ошибка загрузки настроек", f"Не удалось загрузить настройки: {e}")
            self.log_callback(f"Ошибка при загрузке настроек: {e}", "ERROR")
            # Устанавливаем значения по умолчанию
            self.report_path_var.set(str(Path.cwd()))
            self.send_email_var.set(False)
            self.email_recipients_var.set('')
            self.smtp_server_var.set('')
            self.smtp_port_var.set('')
            self.smtp_user_var.set('')
            self.smtp_password_var.set('')
            self.smtp_use_tls_var.set(False)
            self.toggle_email_fields()

    def apply_settings(self) -> None:
        """Применяет и сохраняет текущие настройки."""
        self.log_callback("Применение и сохранение настроек...", "INFO")
        try:
            # Валидация пути
            report_path_str = self.report_path_var.get().strip()
            if not report_path_str:
                messagebox.showwarning("Предупреждение", "Путь сохранения отчётов не может быть пустым.")
                self.log_callback("Путь сохранения отчётов не может быть пустым.", "WARNING")
                return

            report_path = Path(report_path_str)
            if not report_path.is_dir():
                try:
                    report_path.mkdir(parents=True, exist_ok=True)
                    self.log_callback(f"Создана директория для отчётов: {report_path}", "INFO")
                except OSError as e:
                    messagebox.showerror("Ошибка", f"Не удалось создать директорию: {e}")
                    self.log_callback(f"Не удалось создать директорию для отчётов: {e}", "ERROR")
                    return

            # Сохранение пути
            try:
                self.config_manager.get_setting('app.report_dir', str(report_path))
            except Exception:
                logger.exception("config_manager.set_setting('app.report_dir', ...) failed")

            # Обновляем pipeline.output_dir, если можно
            if self.pipeline is not None and hasattr(self.pipeline, 'output_dir'):
                try:
                    setattr(self.pipeline, 'output_dir', report_path)
                except Exception:
                    logger.exception("Не удалось установить pipeline.output_dir")

            # Email
            send_email = bool(self.send_email_var.get())
            try:
                self.config_manager.get_setting('email.send', send_email)
            except Exception:
                logger.exception("config_manager.set_setting('email.send', ...) failed")

            recipients_str = self.email_recipients_var.get().strip()
            if send_email and not recipients_str:
                messagebox.showwarning("Предупреждение", "Если включена отправка по email, укажите хотя бы одного получателя.")
                self.log_callback("Если включена отправка по email, укажите хотя бы одного получателя.", "WARNING")
                return

            try:
                self.config_manager.get_setting('email.recipients', recipients_str)
            except Exception:
                logger.exception("config_manager.set_setting('email.recipients', ...) failed")

            # SMTP
            if send_email:
                smtp_host = self.smtp_server_var.get().strip()
                smtp_port_str = self.smtp_port_var.get().strip()
                smtp_user = self.smtp_user_var.get().strip()
                smtp_password = self.smtp_password_var.get()  # можем получить пустую строку
                smtp_use_tls = bool(self.smtp_use_tls_var.get())

                if not smtp_host or not smtp_port_str or not smtp_user:
                    messagebox.showwarning("Предупреждение", "Если включена отправка по email, необходимо заполнить все поля SMTP (сервер, порт, логин).")
                    self.log_callback("Если включена отправка по email, необходимо заполнить все поля SMTP.", "WARNING")
                    return

                try:
                    smtp_port = int(smtp_port_str)
                    if not (1 <= smtp_port <= 65535):
                        raise ValueError("Порт должен быть в диапазоне от 1 до 65535.")
                except ValueError as e:
                    messagebox.showwarning("Предупреждение", f"Некорректный порт SMTP: {e}")
                    self.log_callback(f"Некорректный порт SMTP: {e}", "WARNING")
                    return

                try:
                    self.config_manager.get_setting('smtp.host', smtp_host)
                    self.config_manager.get_setting('smtp.port', smtp_port)
                    self.config_manager.get_setting('smtp.user', smtp_user)
                    self.config_manager.get_setting('smtp.use_tls', smtp_use_tls)
                except Exception:
                    logger.exception("config_manager.set_setting for SMTP settings failed")

                # Сохранение пароля: только если пользователь ввёл значение — иначе не менять существующее
                if smtp_password:
                    try:
                        self.config_manager.get_setting('smtp.password', smtp_password)
                        self.log_callback("Пароль SMTP обновлен.", "INFO")
                    except Exception:
                        logger.exception("config_manager.set_setting('smtp.password', ...) failed")
            else:
                # Если отправка email выключена — очищаем связанные поля
                try:
                    self.config_manager.set_setting('email.recipients', '')
                    self.config_manager.set_setting('smtp.host', '')
                    self.config_manager.set_setting('smtp.port', '')
                    self.config_manager.set_setting('smtp.user', '')
                    self.config_manager.set_setting('smtp.password', '')
                    self.config_manager.set_setting('smtp.use_tls', False)
                except Exception:
                    logger.exception("config_manager clearing of email/smtp settings failed")

            # Сохранение конфигурации (если есть метод)
            try:
                if hasattr(self.config_manager, "save_config"):
                    self.config_manager.save_config()  # type: ignore
            except Exception:
                logger.exception("config_manager.save_config() failed")

            messagebox.showinfo("Успех", "Настройки успешно обновлены и сохранены.")
            self.log_callback("Настройки отчётов, email и SMTP применены и сохранены.", "INFO")

        except Exception as e:
            logger.exception("Ошибка при применении настроек")
            messagebox.showerror("Ошибка применения настроек", f"Не удалось сохранить настройки: {e}")
            self.log_callback(f"Ошибка при применении настроек: {e}", "ERROR")

    def reset_settings(self) -> None:
        """Сбрасывает настройки к текущим сохраненным значениям."""
        self.load_settings()
        messagebox.showinfo("Сброс", "Настройки сброшены к текущим сохраненным значениям.")
        self.log_callback("Настройки отчётов, email и SMTP сброшены к сохраненным значениям.", "INFO")
