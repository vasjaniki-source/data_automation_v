import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import logging
from typing import Any
import pandas as pd
from pathlib import Path
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
import matplotlib.pyplot as plt
from PIL import Image, ImageTk  # используется только если потребуется вставлять сохранённое изображение

logger = logging.getLogger(__name__)

class VisualizationPanelWidget(ttk.Frame):
    def __init__(self, parent, pipeline):
        super().__init__(parent, padding="8")
        self.pipeline = pipeline
        self.current_plot_path = None
        self._canvas = None
        self._figure = None
        self.create_widgets()

    def create_widgets(self):
        # Контролы
        ctrl_frame = ttk.LabelFrame(self, text="Визуализация и Анализ", padding="8")
        ctrl_frame.pack(fill="x", side="top", pady=6)

        ttk.Label(ctrl_frame, text="Тип графика:").grid(row=0, column=0, padx=5, pady=4, sticky="w")
        available_charts = ["Гистограмма", "Scatter Plot", "Корреляция", "Статистика"]
        self.chart_type = ttk.Combobox(ctrl_frame, values=available_charts, state="readonly", width=20)
        self.chart_type.grid(row=0, column=1, padx=5, pady=4, sticky="w")
        self.chart_type.current(0)

        ttk.Button(ctrl_frame, text="Построить график", command=self.plot_chart).grid(row=0, column=2, padx=6)
        ttk.Button(ctrl_frame, text="Сохранить график", command=self.save_chart_image).grid(row=0, column=3, padx=6)

        # Анализ
        analysis_frame = ttk.LabelFrame(self, text="Анализ данных", padding="8")
        analysis_frame.pack(fill="x", side="top", pady=6)
        ttk.Button(analysis_frame, text="Полный анализ", command=self.run_full_analysis_script).pack(side="left", padx=6)
        ttk.Button(analysis_frame, text="Выборочный анализ", command=self.run_selective_analysis_script).pack(side="left", padx=6)

        # Область для графика (canvas)
        self.plot_container = ttk.Frame(self)
        self.plot_container.pack(fill="both", expand=True, pady=8)

        # Текстовая область для вывода результатов анализа / статистики
        info_frame = ttk.LabelFrame(self, text="Результаты анализа / Сводка", padding="6")
        info_frame.pack(fill="x", side="bottom", pady=6)
        self.info_text = tk.Text(info_frame, height=8, wrap="word")
        self.info_text.pack(fill="both", expand=True)

    def _clear_plot(self):
        """Удаляет текущую фигуру/канвас из контейнера."""
        if self._canvas:
            try:
                self._canvas.get_tk_widget().destroy()
            except Exception:
                pass
            self._canvas = None
            self._figure = None

    def plot_chart(self):
        """Генерирует и отображает выбранный тип графика внутри Tk."""
        if self.pipeline.current_df is None:
            messagebox.showwarning("Внимание", "Нет загруженных данных. Загрузите данные прежде чем строить график.")
            return

        df = self.pipeline.current_df
        selected = self.chart_type.get()
        self._clear_plot()

        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)

        try:
            numeric = df.select_dtypes(include=['number'])
            if selected == "Гистограмма":
                if numeric.empty:
                    messagebox.showwarning("Внимание", "Нет числовых колонок для гистограммы.")
                    return
                col = numeric.columns[0]
                sns.histplot(data=df, x=col, kde=True, ax=ax)
                ax.set_title(f"Гистограмма: {col}")

            elif selected == "Scatter Plot":
                if numeric.shape[1] < 2:
                    messagebox.showwarning("Внимание", "Для scatter plot требуется минимум 2 числовые колонки.")
                    return
                x_col, y_col = numeric.columns[0], numeric.columns[1]
                sns.scatterplot(data=df, x=x_col, y=y_col, ax=ax)
                ax.set_title(f"Scatter: {x_col} vs {y_col}")

            elif selected == "Корреляция":
                if numeric.empty:
                    messagebox.showwarning("Внимание", "Нет числовых колонок для корреляции.")
                    return
                corr = numeric.corr()
                sns.heatmap(corr, annot=True, cmap='coolwarm', ax=ax)
                ax.set_title("Матрица корреляций")

            elif selected == "Статистика":
                # Показываем статистику как текст в info_text, а не как график
                stats = self.pipeline.analyzer.get_extended_stats(df)
                self._update_info_text(stats)
                # Отдельно отрисуем пустой график с сообщением
                ax.text(0.5, 0.5, "Статистическая сводка внизу", ha='center', va='center', fontsize=12)
                ax.axis('off')
            else:
                ax.text(0.5, 0.5, f"Неизвестный тип графика: {selected}", ha='center', va='center')
                ax.axis('off')

            # Встраиваем фигуру в Tkinter
            self._figure = fig
            self._canvas = FigureCanvasTkAgg(fig, master=self.plot_container)
            self._canvas.draw()
            self._canvas.get_tk_widget().pack(fill="both", expand=True)
            self.current_plot_path = None  # текущая фигура в памяти, путь не назначен
        except Exception as e:
            logger.error(f"Ошибка при построении графика: {e}", exc_info=True)
            messagebox.showerror("Ошибка построения графика", str(e))

    def save_chart_image(self):
        """Сохраняет текущую фигуру (если есть) в файл."""
        if self._figure is None:
            messagebox.showwarning("Внимание", "Нет текущего графика для сохранения.")
            return

        initialdir = Path(self.pipeline.output_dir) if hasattr(self.pipeline, 'output_dir') else Path.cwd()
        file_path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG files", "*.png"), ("JPEG files", "*.jpg"), ("All files", "*.*")],
            initialdir=str(initialdir),
            title="Сохранить график как..."
        )
        if not file_path:
            return

        try:
            self._figure.savefig(file_path, bbox_inches='tight')
            self.current_plot_path = file_path
            messagebox.showinfo("Сохранено", f"График сохранён: {file_path}")
            logger.info(f"График сохранён: {file_path}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении графика: {e}", exc_info=True)
            messagebox.showerror("Ошибка сохранения", str(e))

    def _update_info_text(self, data: Any):
        """Удобное форматированное отображение словарей/результатов в текстовой области."""
        self.info_text.delete("1.0", tk.END)
        try:
            if data is None:
                self.info_text.insert(tk.END, "Нет данных для отображения.")
                return
            if isinstance(data, dict):
                import json
                pretty = json.dumps(data, ensure_ascii=False, indent=2)
                self.info_text.insert(tk.END, pretty)
            else:
                self.info_text.insert(tk.END, str(data))
        except Exception as e:
            self.info_text.insert(tk.END, str(data))

    def run_full_analysis_script(self):
        """Вызывает полный анализ через pipeline и отображает результаты."""
        if self.pipeline.current_df is None:
            messagebox.showwarning("Внимание", "Нет загруженных данных для анализа.")
            return

        try:
            # Получаем параметры анализа из config_manager, если есть
            target_col = None
            date_col = None
            try:
                target_col = self.pipeline.config_manager.get_setting('analysis.target_column', None)
            except Exception:
                target_col = self.pipeline.config_manager.get('analysis.target_column', None) if hasattr(self.pipeline.config_manager, 'get') else None
            try:
                date_col = self.pipeline.config_manager.get_setting('analysis.date_column', None)
            except Exception:
                date_col = self.pipeline.config_manager.get('analysis.date_column', None) if hasattr(self.pipeline.config_manager, 'get') else None

            # Запускаем полный анализ; он сохранит результаты в pipeline.analysis_results
            results = self.pipeline.run_full_analysis(target_col=target_col, date_col=date_col)
            # Отображаем текстовую сводку
            self._update_info_text(results)

            # Создаём стандартные картинки (они будут сохранены в папку visualizations)
            vis_dir = Path(self.pipeline.output_dir) / "visualizations"
            vis_paths = []
            try:
                vis_paths = self.pipeline.analyzer.create_plots(self.pipeline.current_df, output_dir=str(vis_dir))
            except Exception as e:
                logger.warning(f"Не удалось сгенерировать файлы визуализаций: {e}")

            # Попробуем отрисовать корреляцию прямо в виджете, если есть числовые колонки
            self.chart_type.set("Корреляция")
            self.plot_chart()

            messagebox.showinfo("Анализ завершен", "Полный анализ выполнен. Результаты показаны внизу и график обновлён.")
        except Exception as e:
            logger.error(f"Ошибка полного анализа: {e}", exc_info=True)
            messagebox.showerror("Ошибка анализа", str(e))

    def run_selective_analysis_script(self):
        """Выполняет выборочный анализ (модули задаются в конфиге или по умолчанию)."""
        if self.pipeline.current_df is None:
            messagebox.showwarning("Внимание", "Нет загруженных данных для анализа.")
            return

        try:
            selective_params = {}
            # Пытаемся получить параметры из config_manager
            try:
                selective_params = self.pipeline.config_manager.get('analysis.selective_parameters', {})
            except Exception:
                try:
                    selective_params = self.pipeline.config_manager.get_setting('analysis.selective_parameters', {})
                except Exception:
                    selective_params = {}

            modules = selective_params.get('modules', None)  # например ['statistics','correlations']
            # Вызов run_selective_analysis (если реализовано в анализаторе)
            if hasattr(self.pipeline.analyzer, 'run_selective_analysis'):
                results = self.pipeline.analyzer.run_selective_analysis(self.pipeline.current_df, modules=modules, **selective_params)
                # Обновляем pipeline.analysis_results (опционально)
                self.pipeline.analysis_results = results
                self._update_info_text(results)
                # Если в результатах есть корреляция, нарисуем её
                if 'correlations' in results and results['correlations']:
                    self.chart_type.set("Корреляция")
                    self.plot_chart()
                else:
                    # Нарисуем гистограмму по умолчанию
                    self.chart_type.set("Гистограмма")
                    self.plot_chart()

                messagebox.showinfo("Анализ завершен", "Выборочный анализ выполнен.")
            else:
                messagebox.showerror("Ошибка", "Анализатор не поддерживает выборочный анализ.")
        except Exception as e:
            logger.error(f"Ошибка выборочного анализа: {e}", exc_info=True)
            messagebox.showerror("Ошибка анализа", str(e))
