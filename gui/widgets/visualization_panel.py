import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import logging
from typing import Any, Optional, List, Callable
from unittest.mock import Mock
import pandas as pd
from pathlib import Path
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
import matplotlib.pyplot as plt
from PIL import Image, ImageTk

logger = logging.getLogger(__name__)


class VisualizationPanelWidget(ttk.Frame):
    """
    Виджет визуализации и экспорта отчётов.

    Взаимодействует с внешним pipeline:
      - pipeline.current_df (pd.DataFrame)
      - pipeline.analysis_results (dict)
      - pipeline.analyzer (для генерации дополнительных графиков, опционально)
      - pipeline.report_manager (ReportManager) для генерации PDF/Excel и отправки e-mail
      - pipeline.config_manager для получения настроек (поддерживаются get_setting и get)
      - pipeline.output_dir для дефолтной директории сохранения
    """

    # Добавляем log_callback для взаимодействия с основной системой логирования GUI
    def __init__(self, parent, pipeline, log_callback: Callable[[str, str], None]):
        super().__init__(parent, padding="8")
        self.pipeline = pipeline
        self.log_callback = log_callback
        self.current_plot_path: Optional[str] = None
        self._canvas = None
        self._figure = None
        self.last_report_paths: Optional[dict] = None  # Храним последние сгенерированные пути
        self.create_widgets()
        self.log_callback("Панель визуализации и отчётов готова.", "INFO")

    def create_widgets(self):
        ctrl_frame = ttk.LabelFrame(self, text="Визуализация и Анализ", padding="8")
        ctrl_frame.pack(fill="x", side="top", pady=6)

        ttk.Label(ctrl_frame, text="Тип графика:").grid(row=0, column=0, padx=5, pady=4, sticky="w")
        available_charts = ["Гистограмма", "Scatter Plot", "Корреляция", "Статистика"]
        self.chart_type = ttk.Combobox(ctrl_frame, values=available_charts, state="readonly", width=20)
        self.chart_type.grid(row=0, column=1, padx=5, pady=4, sticky="w")
        self.chart_type.current(0)
        self.chart_type.bind("<<ComboboxSelected>>", self._handle_chart_type_selection)

        ttk.Button(ctrl_frame, text="Построить график", command=self.plot_chart).grid(row=0, column=2, padx=6)
        ttk.Button(ctrl_frame, text="Сохранить график", command=self.save_chart_image).grid(row=0, column=3, padx=6)

        analysis_frame = ttk.LabelFrame(self, text="Анализ данных", padding="8")
        analysis_frame.pack(fill="x", side="top", pady=6)
        ttk.Button(analysis_frame, text="Полный анализ", command=self.run_full_analysis).pack(side="left", padx=6)
        ttk.Button(analysis_frame, text="Выборочный анализ", command=self.run_selective_analysis).pack(side="left", padx=6)

        export_frame = ttk.LabelFrame(self, text="Экспорт и отправка отчетов", padding="10")
        export_frame.pack(fill="x", side="top", pady=10)

        self.btn_export = ttk.Button(export_frame, text="🚀 Сгенерировать и отправить отчет", command=self.handle_export)
        self.btn_export.pack(side="left", padx=5)

        self.btn_send_email = ttk.Button(export_frame, text="✉️ Отправить по e-mail", command=self.handle_send_email)
        self.btn_send_email.pack(side="left", padx=5)

        self.progress_label = ttk.Label(export_frame, text="Ожидание...", foreground="gray")
        self.progress_label.pack(side="right", padx=10)

        self.plot_container = ttk.Frame(self)
        self.plot_container.pack(fill="both", expand=True, pady=8)

        info_frame = ttk.LabelFrame(self, text="Результаты анализа / Сводка", padding="6")
        info_frame.pack(fill="x", side="bottom", pady=6)
        self.info_text = tk.Text(info_frame, height=8, wrap="word", font=("Consolas", 9))
        self.info_text.pack(fill="both", expand=True)

    def _handle_chart_type_selection(self, event=None):
        """Обрабатывает выбор типа графика в комбобоксе, обновляет info_text, если выбран 'Статистика'."""
        selected = self.chart_type.get()
        if selected == "Статистика":
            # Если выбран "Статистика", сразу показываем результаты, не строим график
            if self.pipeline.current_df is not None:
                try:
                    stats = self.pipeline.analyzer.get_extended_stats(self.pipeline.current_df)
                    self._update_info_text(stats)
                    self._clear_plot() # Очищаем график, так как для статистики его нет
                except Exception as e:
                    self.log_callback(f"Ошибка получения статистики: {e}", "ERROR")
                    self._update_info_text(f"Ошибка получения статистики: {e}")
            else:
                self._update_info_text("Нет данных для отображения статистики.")
                self._clear_plot()
        else:
            # Для других типов графиков, очищаем текстовую область
            self._update_info_text("")


    def _clear_plot(self):
        """Удаляет текущую фигуру/канвас из контейнера."""
        if self._canvas:
            try:
                self._canvas.get_tk_widget().destroy()
            except Exception as e:
                logger.warning(f"Ошибка при уничтожении tk_widget: {e}")
            self._canvas = None
            self._figure = None
        # Также очищаем plot_container на всякий случай, если там что-то осталось
        for widget in self.plot_container.winfo_children():
            widget.destroy()

    def plot_chart(self):
        """Генерирует и отображает выбранный тип графика внутри Tk."""
        self.log_callback("Построение графика...", "INFO")
        if self.pipeline.current_df is None or self.pipeline.current_df.empty:
            messagebox.showwarning("Внимание", "Нет загруженных данных. Загрузите данные прежде чем строить график.")
            self.log_callback("Попытка построить график без данных.", "WARNING")
            return

        df = self.pipeline.current_df.copy() # Работаем с копией, чтобы избежать нежелательных изменений
        selected = self.chart_type.get()
        
        # Если выбран "Статистика", то отображаем текст и не строим график
        if selected == "Статистика":
            try:
                stats = self.pipeline.analyzer.get_extended_stats(df)
                self._update_info_text(stats)
                messagebox.showinfo("Статистика", "Статистическая сводка отображена в текстовой области.")
                self.log_callback("Отображена статистическая сводка.", "INFO")
                self._clear_plot() # Очищаем область графика
            except Exception as e:
                logger.error(f"Ошибка при получении расширенной статистики: {e}", exc_info=True)
                messagebox.showerror("Ошибка", f"Не удалось получить статистику: {e}")
                self.log_callback(f"Ошибка при получении расширенной статистики: {e}", "ERROR")
            return

        self._clear_plot() # Очищаем предыдущий график
        self._update_info_text("") # Очищаем текстовую область, если не "Статистика"

        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)

        try:
            numeric_df = df.select_dtypes(include=['number'])
            
            if selected == "Гистограмма":
                if numeric_df.empty:
                    messagebox.showwarning("Внимание", "Нет числовых колонок для гистограммы.")
                    self.log_callback("Нет числовых колонок для гистограммы.", "WARNING")
                    return
                col = numeric_df.columns[0] # Берем первую числовую колонку
                sns.histplot(data=df, x=col, kde=True, ax=ax)
                ax.set_title(f"Гистограмма: {col}")

            elif selected == "Scatter Plot":
                if numeric_df.shape[1] < 2:
                    messagebox.showwarning("Внимание", "Для scatter plot требуется минимум 2 числовые колонки.")
                    self.log_callback("Нет достаточного количества числовых колонок для scatter plot.", "WARNING")
                    return
                x_col, y_col = numeric_df.columns[0], numeric_df.columns[1] # Берем первые две
                sns.scatterplot(data=df, x=x_col, y=y_col, ax=ax)
                ax.set_title(f"Scatter: {x_col} vs {y_col}")

            elif selected == "Корреляция":
                if numeric_df.empty:
                    messagebox.showwarning("Внимание", "Нет числовых колонок для корреляции.")
                    self.log_callback("Нет числовых колонок для корреляции.", "WARNING")
                    return
                corr = numeric_df.corr()
                sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5, ax=ax)
                ax.set_title("Матрица корреляций")

            else:
                ax.text(0.5, 0.5, f"Неизвестный тип графика: {selected}", ha='center', va='center', transform=ax.transAxes)
                ax.axis('off')
            
            fig.tight_layout() # Автоматически подстраивает макет
            # Встраиваем фигуру в Tkinter
            self._figure = fig
            self._canvas = FigureCanvasTkAgg(fig, master=self.plot_container)
            self._canvas.draw()
            self._canvas.get_tk_widget().pack(fill="both", expand=True)
            self.current_plot_path = None  # текущая фигура в памяти, путь не назначен
            self.log_callback(f"График типа '{selected}' успешно построен.", "INFO")
        except Exception as e:
            logger.error(f"Ошибка при построении графика '{selected}': {e}", exc_info=True)
            messagebox.showerror("Ошибка построения графика", f"Не удалось построить график '{selected}': {e}")
            self.log_callback(f"Ошибка при построении графика '{selected}': {e}", "ERROR")

    def save_chart_image(self):
        """Сохраняет текущую фигуру (если есть) в файл."""
        if self._figure is None:
            messagebox.showwarning("Внимание", "Нет текущего графика для сохранения.")
            self.log_callback("Попытка сохранить график, когда его нет.", "WARNING")
            return

        # Использование config_manager для получения output_dir
        initialdir_str = self._read_config_value('app.report_dir', default=str(Path.cwd()))
        initialdir = Path(initialdir_str)
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG files", "*.png"), ("JPEG files", "*.jpg"), ("All files", "*.*")],
            initialdir=str(initialdir),
            title="Сохранить график как..."
        )
        if not file_path:
            self.log_callback("Сохранение графика отменено пользователем.", "INFO")
            return

        try:
            self._figure.savefig(file_path, bbox_inches='tight')
            self.current_plot_path = file_path
            messagebox.showinfo("Сохранено", f"График сохранён: {file_path}")
            self.log_callback(f"График сохранён: {file_path}", "INFO")
        except Exception as e:
            logger.error(f"Ошибка при сохранении графика: {e}", exc_info=True)
            messagebox.showerror("Ошибка сохранения", f"Не удалось сохранить график: {e}")
            self.log_callback(f"Ошибка при сохранении графика: {e}", "ERROR")

    def _update_info_text(self, data: Any):
        """Удобное форматированное отображение словарей/результатов в текстовой области."""
        self.info_text.config(state="normal")
        self.info_text.delete("1.0", tk.END)
        try:
            if data is None or data == "":
                self.info_text.insert(tk.END, "Нет данных для отображения.")
                self.info_text.config(state="disabled")
                return
            if isinstance(data, dict):
                import json
                pretty = json.dumps(data, ensure_ascii=False, indent=2)
                self.info_text.insert(tk.END, pretty)
            else:
                self.info_text.insert(tk.END, str(data))
        except Exception as e:
            logger.error(f"Ошибка при форматировании данных для info_text: {e}", exc_info=True)
            self.info_text.insert(tk.END, f"Ошибка отображения данных: {e}")
        finally:
            self.info_text.config(state="disabled")

    def run_full_analysis(self):
        """Вызывает полный анализ через pipeline и отображает результаты."""
        self.log_callback("Запуск полного анализа...", "INFO")
        if self.pipeline.current_df is None or self.pipeline.current_df.empty:
            messagebox.showwarning("Внимание", "Нет загруженных данных для анализа.")
            self.log_callback("Попытка полного анализа без данных.", "WARNING")
            return
        
        # Проверяем, что pipeline не является Mock-объектом
        if isinstance(self.pipeline, Mock):
            messagebox.showwarning("Внимание", "Пайплайн не инициализирован. Анализ данных недоступен.")
            self.log_callback("Пайплайн не инициализирован. Анализ данных недоступен.", "WARNING")
            return

        try:
            # Получаем параметры анализа из config_manager
            target_col = self._read_config_value('analysis.target_column', default=None)
            date_col = self._read_config_value('analysis.date_column', default=None)

            # Запускаем полный анализ; он сохранит результаты в pipeline.analysis_results
            self.progress_label.config(text="⏳ Выполнение полного анализа...", foreground="blue")
            self.update_idletasks() # Обновляем GUI
            
            results = self.pipeline.run_full_analysis(target_col=target_col, date_col=date_col)
            # Отображаем текстовую сводку
            self._update_info_text(results)

            # Создаём стандартные картинки через analyzer и сохраняем их
            vis_dir = Path(self._read_config_value('app.report_dir', default=str(Path.cwd()))) / "visualizations"
            vis_dir.mkdir(parents=True, exist_ok=True)
            if hasattr(self.pipeline.analyzer, 'create_plots'):
                try:
                    _ = self.pipeline.analyzer.create_plots(self.pipeline.current_df, output_dir=str(vis_dir))
                    self.log_callback(f"Дополнительные визуализации сохранены в {vis_dir}", "INFO")
                except Exception as e:
                    logger.warning(f"Не удалось сгенерировать дополнительные визуализации: {e}")
                    self.log_callback(f"Ошибка при генерации дополнительных визуализаций: {e}", "WARNING")

            # Попробуем отрисовать корреляцию прямо в виджете, если есть числовые колонки
            # Или другой график по умолчанию, например гистограмму
            self.chart_type.set("Корреляция") # Устанавливаем и вызываем plot_chart
            self.plot_chart()

            messagebox.showinfo("Анализ завершен", "Полный анализ выполнен. Результаты показаны внизу и график обновлён.")
            self.log_callback("Полный анализ данных завершен.", "INFO")
        except Exception as e:
            logger.error(f"Ошибка полного анализа: {e}", exc_info=True)
            messagebox.showerror("Ошибка анализа", f"Не удалось выполнить полный анализ: {e}")
            self.log_callback(f"Ошибка полного анализа: {e}", "ERROR")
        finally:
            self.progress_label.config(text="Ожидание...", foreground="gray")

    def run_selective_analysis(self):
        """Выполняет выборочный анализ (модули задаются в конфиге или по умолчанию)."""
        self.log_callback("Запуск выборочного анализа...", "INFO")
        if self.pipeline.current_df is None or self.pipeline.current_df.empty:
            messagebox.showwarning("Внимание", "Нет загруженных данных для анализа.")
            self.log_callback("Попытка выборочного анализа без данных.", "WARNING")
            return
        
        # Проверяем, что pipeline не является Mock-объектом
        if isinstance(self.pipeline, Mock):
            messagebox.showwarning("Внимание", "Пайплайн не инициализирован. Выборочный анализ данных недоступен.")
            self.log_callback("Пайплайн не инициализирован. Выборочный анализ данных недоступен.", "WARNING")
            return

        try:
            selective_params = self._read_config_value('analysis.selective_parameters', default={})
            modules = selective_params.get('modules', None)  # например ['statistics','correlations']

            self.progress_label.config(text="⏳ Выполнение выборочного анализа...", foreground="blue")
            self.update_idletasks() # Обновляем GUI

            if hasattr(self.pipeline.analyzer, 'run_selective_analysis'):
                results = self.pipeline.analyzer.run_selective_analysis(self.pipeline.current_df, modules=modules, **selective_params)
                self.pipeline.analysis_results = results # Обновляем pipeline.analysis_results
                self._update_info_text(results)
                
                # Если в результатах есть корреляция, нарисуем её, иначе гистограмму
                if 'correlations' in results and results['correlations'] is not None:
                    self.chart_type.set("Корреляция")
                    self.plot_chart()
                elif not self.pipeline.current_df.select_dtypes(include=['number']).empty:
                    self.chart_type.set("Гистограмма")
                    self.plot_chart()
                else:
                    self._clear_plot() # Если нет числовых данных, очищаем график
                    messagebox.showinfo("Анализ завершен", "Выборочный анализ выполнен. График не построен, так как нет подходящих данных.")
                    self.log_callback("Выборочный анализ выполнен, график не построен (нет подходящих данных).", "INFO")

                messagebox.showinfo("Анализ завершен", "Выборочный анализ выполнен.")
                self.log_callback("Выборочный анализ данных завершен.", "INFO")
            else:
                messagebox.showerror("Ошибка", "Анализатор не поддерживает выборочный анализ.")
                self.log_callback("Анализатор не поддерживает выборочный анализ.", "ERROR")
        except Exception as e:
            logger.error(f"Ошибка выборочного анализа: {e}", exc_info=True)
            messagebox.showerror("Ошибка анализа", f"Не удалось выполнить выборочный анализ: {e}")
            self.log_callback(f"Ошибка выборочного анализа: {e}", "ERROR")
        finally:
            self.progress_label.config(text="Ожидание...", foreground="gray")

    def _read_config_value(self, primary_key: str, alt_key: Optional[str] = None, default: Any = None):
        cfg = getattr(self.pipeline, 'config_manager', None)
        if cfg is None or isinstance(cfg, Mock):
            return default
        try:
            if hasattr(cfg, 'get_setting'):
                v = cfg.get_setting(primary_key, None)
                if v is not None:
                    return v
        except Exception:
            pass
        try:
            if hasattr(cfg, 'get'):
                if '.' in primary_key:
                    keys = primary_key.split('.')
                    cur = cfg.get(keys[0], {})
                    for k in keys[1:]:
                        if isinstance(cur, dict):
                            cur = cur.get(k)
                        else:
                            cur = None; break
                    if cur is not None:
                        return cur
                else:
                    v = cfg.get(primary_key, None)
                    if v is not None:
                        return v
        except Exception:
            pass
        if alt_key:
            try:
                if hasattr(cfg, 'get_setting'):
                    v = cfg.get_setting(alt_key, None)
                    if v is not None:
                        return v
            except Exception:
                pass
            try:
                if hasattr(cfg, 'get'):
                    if '.' in alt_key:
                        keys = alt_key.split('.')
                        cur = cfg.get(keys[0], {})
                        for k in keys[1:]:
                            if isinstance(cur, dict):
                                cur = cur.get(k)
                            else:
                                cur = None; break
                        if cur is not None:
                            return cur
                    else:
                        v = cfg.get(alt_key, None)
                        if v is not None:
                            return v
            except Exception:
                pass
        return default

    def _get_recipients_from_config(self) -> List[str]:
        recipients_raw = self._read_config_value('smtp.recipients', alt_key='smtp.recipients', default='')
        recipients_list: List[str] = []
        if isinstance(recipients_raw, str):
            recipients_list = [r.strip() for r in recipients_raw.split(',') if r.strip()]
        elif isinstance(recipients_raw, (list, tuple)):
            recipients_list = list(recipients_raw)
        return recipients_list

    def handle_export(self):
        self.log_callback("Запуск генерации и отправки отчётов...", "INFO")
        if self.pipeline.current_df is None or self.pipeline.current_df.empty:
            messagebox.showwarning("Внимание", "Сначала загрузите данные!")
            return
        if not hasattr(self.pipeline, 'analysis_results') or not self.pipeline.analysis_results:
            messagebox.showwarning("Внимание", "Сначала выполните анализ!")
            return
        if isinstance(self.pipeline, Mock) or not hasattr(self.pipeline, 'report_manager'):
            messagebox.showwarning("Внимание", "ReportManager не инициализирован.")
            return

        self.btn_export.config(state="disabled")
        self.progress_label.config(text="⏳ Генерация отчета...", foreground="blue")
        self.update_idletasks()

        try:
            output_dir_str = str(self._read_config_value('app.report_dir', default=str(Path.cwd())))
            output_format = self._read_config_value('report.format', alt_key='settings.report_format', default='pdf')
            send_email = bool(self._read_config_value('smtp.send_email', alt_key='smtp.send_email', default=False))
            recipients_list = self._get_recipients_from_config()
            if send_email and not recipients_list:
                messagebox.showwarning("Внимание", "Включена отправка по email, но не указан ни один получатель. Отправка будет пропущена.")
                send_email = False

            report_paths = self.pipeline.report_manager.generate_reports(
                df=self.pipeline.current_df,
                analysis_results=self.pipeline.analysis_results,
                output_dir=output_dir_str,
                output_format=output_format,
                send_email=send_email,
                email_recipients=recipients_list if recipients_list else None,
                report_name_prefix=self._read_config_value('report.name_prefix', default='Report')
            )

            # Сохраняем результат для последующей отправки (handle_send_email)
            try:
                self.last_report_paths = self.pipeline.report_manager.get_last_generated()
            except Exception:
                self.last_report_paths = dict(report_paths or {})

            results_log = []
            if report_paths:
                for fmt, path in report_paths.items():
                    results_log.append(f"✅ {fmt.upper()}: {path}")
            else:
                results_log.append("⚠️ Отчёты не были созданы.")
            if send_email and recipients_list:
                results_log.append(f"ℹ️ Попытка отправки на: {', '.join(recipients_list)} (см. логи).")

            self.progress_label.config(text="✅ Готово", foreground="green")
            messagebox.showinfo("Экспорт завершен", "\n".join(results_log))
            self.log_callback("Процесс генерации отчётов завершен.", "INFO")

        except Exception as e:
            logger.exception("Ошибка в handle_export: %s", e)
            messagebox.showerror("Ошибка", f"Процесс экспорта прерван: {e}")
            self.progress_label.config(text="❌ Ошибка", foreground="red")
            self.log_callback(f"Процесс экспорта прерван: {e}", "ERROR")
        finally:
            self.btn_export.config(state="normal")
            self.update_idletasks()

    def handle_send_email(self):
        self.log_callback("Инициация отправки отчёта по e-mail...", "INFO")
        if self.pipeline.current_df is None or self.pipeline.current_df.empty:
            messagebox.showwarning("Внимание", "Сначала загрузите данные!")
            return
        if not hasattr(self.pipeline, 'analysis_results') or not self.pipeline.analysis_results:
            messagebox.showwarning("Внимание", "Сначала выполните анализ!")
            return
        if isinstance(self.pipeline, Mock) or not hasattr(self.pipeline, 'report_manager'):
            messagebox.showwarning("Внимание", "ReportManager не инициализирован.")
            return

        recipients_list = self._get_recipients_from_config()
        if not recipients_list:
            messagebox.showwarning("Внимание", "Не указаны получатели (smtp.recipients). Укажите их в настройках.")
            return

        self.btn_send_email.config(state="disabled")
        self.progress_label.config(text="⏳ Отправка email...", foreground="blue")
        self.update_idletasks()

        try:
            rm = self.pipeline.report_manager
            report_paths = getattr(self, 'last_report_paths', None) or (rm.get_last_generated() if hasattr(rm, 'get_last_generated') else {})
            # Оставляем только реально существующие файлы
            report_paths = {k: v for k, v in (report_paths or {}).items() if isinstance(v, str) and Path(v).is_file()}

            if report_paths:
                sent_ok = False
                try:
                    sent_ok = rm.send_reports(report_paths, recipients_list, analysis_results=self.pipeline.analysis_results)
                except Exception as e:
                    self.log_callback(f"Ошибка при вызове send_reports: {e}", "ERROR")
                    sent_ok = False

                if sent_ok:
                    self.progress_label.config(text="✅ Отправлено", foreground="green")
                    messagebox.showinfo("Отправлено", f"Отчёты отправлены на: {', '.join(recipients_list)}")
                    self.log_callback(f"Отчёты отправлены на: {', '.join(recipients_list)}", "INFO")
                else:
                    self.progress_label.config(text="❌ Ошибка", foreground="red")
                    messagebox.showerror("Ошибка отправки", "Не удалось отправить отчёты (см. логи).")
            else:
                # fallback: генерируем + отправляем
                self.progress_label.config(text="🔧 Генерация отчёта перед отправкой...", foreground="blue")
                self.update_idletasks()
                report_format = self._read_config_value('report.format', alt_key='settings.report_format', default='pdf')
                output_dir_str = str(self._read_config_value('app.report_dir', default=str(Path.cwd())))
                try:
                    new_paths = rm.generate_reports(
                        df=self.pipeline.current_df,
                        analysis_results=self.pipeline.analysis_results,
                        output_dir=output_dir_str,
                        output_format=report_format,
                        send_email=True,
                        email_recipients=recipients_list,
                        report_name_prefix=self._read_config_value('report.name_prefix', default='Report')
                    )
                    # Сохраняем last_report_paths
                    try:
                        self.last_report_paths = rm.get_last_generated()
                    except Exception:
                        self.last_report_paths = dict(new_paths or {})

                    self.progress_label.config(text="✅ Отправлено/Сгенерировано", foreground="green")
                    messagebox.showinfo("Отправлено", f"Отчёт сгенерирован и отправлен на: {', '.join(recipients_list)}")
                    self.log_callback("Сгенерирован и отправлен отчёт.", "INFO")
                except Exception as e:
                    logger.exception("Ошибка генерации+отправки: %s", e)
                    self.progress_label.config(text="❌ Ошибка", foreground="red")
                    messagebox.showerror("Ошибка", f"Не удалось сгенерировать и отправить отчёт: {e}")
                    self.log_callback(f"Ошибка генерации+отправки: {e}", "ERROR")
        finally:
            self.btn_send_email.config(state="normal")
            self.update_idletasks()
