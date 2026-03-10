

from tkinter import Toplevel, StringVar, BooleanVar, Entry, Label, Button, Frame, Checkbutton, LabelFrame, messagebox, filedialog
from tkinter import ttk
from tkinter.ttk import Combobox, Style
import logging
import os
import sys
from pathlib import Path
from tkinter import filedialog

logger = logging.getLogger(__name__)

class SettingsPanelWidget(Frame): 
    def __init__(self, parent, pipeline):
        super().__init__(parent, padx=20, pady=20)
        self.pipeline = pipeline
        self.config_manager = pipeline.config_manager

        # --- Переменные для хранения текущих настроек ---
        self.report_path_var = StringVar()
        self.send_email_var = BooleanVar()
        self.email_recipients_var = StringVar()
        
        # --- Новые переменные для SMTP ---
        self.smtp_server_var = StringVar()
        self.smtp_port_var = StringVar() 
        self.smtp_user_var = StringVar()
        self.smtp_password_var = StringVar()
        self.smtp_use_tls_var = BooleanVar() 

        self.create_widgets()
        self.load_settings() 

    def create_widgets(self):
        # --- Раздел "Путь сохранения отчётов" ---
        path_frame = LabelFrame(self, text="Настройки отчётов", padx=20, pady=20)
        path_frame.pack(fill="x", pady=10)

        entry_btn_frame = Frame(path_frame)
        entry_btn_frame.pack(fill="x")

        Label(entry_btn_frame, text="Путь сохранения:").pack(anchor="w", side="left")
        self.report_path_entry = Entry(entry_btn_frame, textvariable=self.report_path_var, width=40)
        self.report_path_entry.pack(anchor="w", side="left", fill="x", expand=True, pady=5, padx=(5, 0))

        Button(entry_btn_frame, text="Обзор...", command=self.browse_report_path).pack(anchor="w", side="left", padx=(5, 0))

        # --- Раздел "Отправка по Email" ---
        email_frame = LabelFrame(self, text="Отправка по Email", padx=10, pady=10)
        email_frame.pack(fill="x", pady=10)

        self.send_email_check = Checkbutton(email_frame, text="Отправлять отчёт по email", variable=self.send_email_var, command=self.toggle_email_fields)
        self.send_email_check.pack(anchor="w", pady=5)

        self.email_recipients_label = Label(email_frame, text="Email получателей (через запятую):")
        self.email_recipients_entry = Entry(email_frame, textvariable=self.email_recipients_var, width=40)
        
        # --- Раздел "Параметры SMTP сервера" ---
        # Этот раздел будет виден только если включена отправка по email
        smtp_frame = LabelFrame(email_frame, text="Настройки SMTP сервера", padx=10, pady=10)
        
        smtp_server_row = Frame(smtp_frame)
        smtp_server_row.pack(fill="x")
        Label(smtp_server_row, text="SMTP сервер:").pack(side="left", anchor="w")
        self.smtp_server_entry = Entry(smtp_server_row, textvariable=self.smtp_server_var, width=30)
        self.smtp_server_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        smtp_port_row = Frame(smtp_frame)
        smtp_port_row.pack(fill="x", pady=5)
        Label(smtp_port_row, text="Порт:").pack(side="left", anchor="w")
        self.smtp_port_entry = Entry(smtp_port_row, textvariable=self.smtp_port_var, width=10)
        self.smtp_port_entry.pack(side="left", padx=5)
        
        smtp_user_row = Frame(smtp_frame)
        smtp_user_row.pack(fill="x")
        Label(smtp_user_row, text="Логин (Email):").pack(side="left", anchor="w")
        self.smtp_user_entry = Entry(smtp_user_row, textvariable=self.smtp_user_var, width=30)
        self.smtp_user_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        smtp_password_row = Frame(smtp_frame)
        smtp_password_row.pack(fill="x", pady=5)
        Label(smtp_password_row, text="Пароль:").pack(side="left", anchor="w")
        self.smtp_password_entry = Entry(smtp_password_row, textvariable=self.smtp_password_var, show="*", width=30)
        self.smtp_password_entry.pack(side="left", fill="x", expand=True, padx=5)

        self.smtp_use_tls_check = Checkbutton(smtp_frame, text="Использовать TLS/STARTTLS", variable=self.smtp_use_tls_var)
        self.smtp_use_tls_check.pack(anchor="w", pady=5)

        self.smtp_frame = smtp_frame # Сохраняем ссылку для показа/скрытия

        # --- Кнопки ---
        btn_frame = Frame(self)
        btn_frame.pack(pady=20)
        Button(btn_frame, text="Применить", command=self.apply_settings).pack(side="left", padx=5)
        Button(btn_frame, text="Сброс", command=self.reset_settings).pack(side="left", padx=5)

    def browse_report_path(self):
        """Открывает диалог выбора директории для сохранения отчётов."""
        current_path = self.report_path_var.get()
        directory = filedialog.askdirectory(initialdir=current_path, title="Выберите папку для сохранения отчётов")
        if directory:
            self.report_path_var.set(directory)
            logger.debug(f"Выбрана новая директория для отчётов: {directory}")

    def toggle_email_fields(self):
        """Показывает/скрывает поля для ввода email и SMTP."""
        if self.send_email_var.get():
            self.email_recipients_label.pack(anchor="w", padx=10)
            self.email_recipients_entry.pack(fill="x", pady=5, padx=10)
            self.smtp_frame.pack(fill="x", pady=5, padx=10) # Показываем SMTP настройки
        else:
            self.email_recipients_label.pack_forget()
            self.email_recipients_entry.pack_forget()
            self.smtp_frame.pack_forget() # Скрываем SMTP настройки

    def load_settings(self):
        """Загружает настройки из pipeline.config_manager."""
        try:
            # Путь сохранения отчётов
            output_dir_str = self.config_manager.get('settings.report_output_dir', str(self.pipeline.output_dir))
            self.report_path_var.set(output_dir_str)
            
            # Настройки Email
            send_email = self.config_manager.get('settings.email.send', False)
            self.send_email_var.set(send_email)
            
            recipients = self.config_manager.get('settings.email.recipients', '')
            self.email_recipients_var.set(recipients)
            
            # --- Новые настройки SMTP ---
            # Получаем настройки из секции 'smtp', если она есть, иначе пробуем 'email.smtp'
            smtp_cfg_from_conf = self.config_manager.get('smtp', {})
            email_smtp_cfg = self.config_manager.get('email.smtp', {}) # Пример, если настройки внутри email
            
            if smtp_cfg_from_conf:
                self.smtp_server_var.set(smtp_cfg_from_conf.get('server', ''))
                self.smtp_port_var.set(str(smtp_cfg_from_conf.get('port', '')))
                self.smtp_user_var.set(smtp_cfg_from_conf.get('user', ''))
                self.smtp_password_var.set(smtp_cfg_from_conf.get('password', ''))
                self.smtp_use_tls_var.set(smtp_cfg_from_conf.get('use_tls', False))
            elif email_smtp_cfg: # Если есть в email.smtp
                self.smtp_server_var.set(email_smtp_cfg.get('server', ''))
                self.smtp_port_var.set(str(email_smtp_cfg.get('port', '')))
                self.smtp_user_var.set(email_smtp_cfg.get('user', ''))
                self.smtp_password_var.set(email_smtp_cfg.get('password', ''))
                self.smtp_use_tls_var.set(email_smtp_cfg.get('use_tls', False))
            else: # Если настроек нет вообще, очищаем поля
                self.smtp_server_var.set('')
                self.smtp_port_var.set('')
                self.smtp_user_var.set('')
                self.smtp_password_var.set('')
                self.smtp_use_tls_var.set(False)
            
            self.toggle_email_fields() # Обновляем видимость полей email и SMTP
            logger.info("Настройки отчётов и email загружены.")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке настроек: {e}", exc_info=True)
            messagebox.showerror("Ошибка загрузки настроек", f"Не удалось загрузить настройки: {e}")

    def apply_settings(self):
        """Применяет и сохраняет текущие настройки."""
        try:
            # --- Валидация пути ---
            report_path = self.report_path_var.get().strip()
            if not report_path:
                messagebox.showwarning("Предупреждение", "Путь сохранения отчётов не может быть пустым.")
                return
            
            # --- Сохранение пути ---
            self.config_manager.set('settings.report_output_dir', report_path)
            self.pipeline.output_dir = Path(report_path) # Обновляем и в pipeline
            
            # --- Валидация и сохранение настроек email ---
            send_email = self.send_email_var.get()
            self.config_manager.set('settings.email.send', send_email)
            
            recipients_str = self.email_recipients_var.get().strip()
            if send_email and not recipients_str:
                messagebox.showwarning("Предупреждение", "Если включена отправка по email, укажите хотя бы одного получателя.")
                return
            self.config_manager.set('settings.email.recipients', recipients_str)
            
            # --- Валидация и сохранение настроек SMTP ---
            if send_email:
                smtp_server = self.smtp_server_var.get().strip()
                smtp_port_str = self.smtp_port_var.get().strip()
                smtp_user = self.smtp_user_var.get().strip()
                smtp_password = self.smtp_password_var.get().strip() # Пароль может быть пустым, если он не меняется
                smtp_use_tls = self.smtp_use_tls_var.get()

                if not smtp_server or not smtp_port_str or not smtp_user:
                    messagebox.showwarning("Предупреждение", "Если включена отправка по email, необходимо заполнить все поля SMTP (сервер, порт, логин).")
                    return

                try:
                    smtp_port = int(smtp_port_str)
                    if not (1 <= smtp_port <= 65535):
                        raise ValueError("Порт должен быть в диапазоне от 1 до 65535.")
                except ValueError as e:
                    messagebox.showwarning("Предупреждение", f"Некорректный порт SMTP: {e}")
                    return

                # --- Сохранение SMTP настроек ---
                # Сохраняем в секцию 'smtp', так как ReportManager теперь ищет там.
                # Если ReportManager будет искать в 'email.smtp', измените ключи сохранения.
                self.config_manager.set('smtp.server', smtp_server)
                self.config_manager.set('smtp.port', smtp_port)
                self.config_manager.set('smtp.user', smtp_user)
                # Сохраняем пароль, только если он был изменен или его не было раньше.
                # Это может потребовать более сложной логики, если нужно отличать "не изменен" от "введен пустой".
                # Простой подход: всегда перезаписывать, если поле заполнено.
                # Или, если пароль не был изменен, не перезаписывать его.
                # Для простоты: сохраняем, если поле НЕ пустое.
                if smtp_password:
                    self.config_manager.set('smtp.password', smtp_password)
                # Если пароль был ранее, но поле сейчас пустое, это может быть проблемой.
                # Возможно, нужно проверять, был ли пароль в config_manager раньше,
                # и если поле SMTP пустое, а пароль был, то не удалять его.
                # Для безопасности, если пароль не введен, лучше его не трогать.
                
                self.config_manager.set('smtp.use_tls', smtp_use_tls)

            else: # Если отправка email выключена, очищаем соответствующие настройки SMTP, чтобы они не мешали
                self.config_manager.delete('smtp.server')
                self.config_manager.delete('smtp.port')
                self.config_manager.delete('smtp.user')
                self.config_manager.delete('smtp.password')
                self.config_manager.delete('smtp.use_tls')

            messagebox.showinfo("Успех", "Настройки успешно обновлены и сохранены.")
            logger.info("Настройки отчётов, email и SMTP применены и сохранены.")
            
        except Exception as e:
            logger.error(f"Ошибка при применении настроек: {e}", exc_info=True)
            messagebox.showerror("Ошибка применения настроек", f"Не удалось сохранить настройки: {e}")

    def reset_settings(self):
        """Сбрасывает настройки к текущим сохраненным значениям."""
        self.load_settings()
        messagebox.showinfo("Сброс", "Настройки сброшены к текущим сохраненным значениям.")
        logger.info("Настройки отчётов, email и SMTP сброшены.")

