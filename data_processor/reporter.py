import os
import sys
from matplotlib.figure import Figure
import numpy as np
import pandas as pd
import logging
import tempfile
import shutil
from typing import Dict, Any, Optional, List
from datetime import datetime

# ReportLab для PDF
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm

# Matplotlib/Seaborn для графиков
import matplotlib.pyplot as plt
import seaborn as sns

# Email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# OpenPyxl для форматирования Excel
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Alignment

logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Исключение для ошибок валидации данных"""
    pass

class PDFReportGenerator:
    def __init__(self):
        self.default_font = 'Helvetica'
        self.font_name = 'DejaVuSans'
        self.font_path = self._find_font_path()

        if self.font_path:
            self._register_font(self.font_path)
        # Шрифт по умолчанию (Helvetica) доступен в ReportLab изначально — дополнительная регистрация не нужна

    def _find_font_path(self):
        """Ищет шрифт DejaVuSans.ttf в стандартных локациях."""
        candidates = [
            # Локальные пути
            os.path.join(os.path.dirname(__file__), 'DejaVuSans.ttf'),
            os.path.join(os.path.dirname(__file__), 'fonts', 'DejaVuSans.ttf'),

            # Системные пути Linux
            '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
            '/usr/local/share/fonts/DejaVuSans.ttf',

            # macOS
            '/Library/Fonts/DejaVuSans.ttf',
            os.path.expanduser('~/Library/Fonts/DejaVuSans.ttf'),

            # Windows
            'C:\\Windows\\Fonts\\DejaVuSans.ttf',

            # Виртуальные окружения и matplotlib
            os.path.join(sys.prefix, 'Lib', 'site-packages',
                         'matplotlib', 'mpl-data', 'fonts', 'ttf', 'DejaVuSans.ttf'),
        ]

        # Добавляем путь из matplotlib, если библиотека установлена
        try:
            import matplotlib
            mpl_path = os.path.join(matplotlib.get_data_path(), 'fonts', 'ttf', 'DejaVuSans.ttf')
            if os.path.exists(mpl_path):
                candidates.insert(0, mpl_path)  # Приоритет matplotlib
        except ImportError:
            pass

        for path in candidates:
            normalized = os.path.expanduser(path)
            if os.path.isfile(normalized):
                return normalized
        return None

    def _register_font(self, font_path):
        """Регистрирует шрифт в ReportLab."""
        try:
            pdfmetrics.registerFont(TTFont(self.font_name, font_path))
        except Exception as e:
            raise RuntimeError(f"Не удалось зарегистрировать шрифт {self.font_name}: {e}")


    def _validate_analysis_results(self, analysis_results: Dict[str, Any]) -> bool:
        """Валидация структуры analysis_results"""
        required_keys = ['statistics', 'ml_model'] # summary генерируется в ReportManager
        for key in required_keys:
            if key not in analysis_results:
                logger.warning(f"Отсутствует обязательный ключ '{key}' в analysis_results")
                return False
        return True

    def create_pdf_report(
        self,
        analysis_results: Dict[str, Any],
        df: Optional[pd.DataFrame],
        output_path: str
    ) -> str:
        if not self._validate_analysis_results(analysis_results):
            raise ValidationError("Некорректная структура analysis_results")

        c = canvas.Canvas(output_path, pagesize=A4)
        width, height = A4
        c.setFont(self.default_font, 12)

        title = "ОТЧЁТ АНАЛИЗА ДАННЫХ"
        # Для центрирования текста
        c.drawCentredString(width / 2, height - 30 * mm, title)
        y_position = height - 50 * mm

        # 1. Сводка анализа
        self._add_section_header(c, "1. СВОДКА АНАЛИЗА", y_position)
        y_position = self._add_summary_section(c, analysis_results, y_position - 10 * mm)
        y_position = self._check_and_add_page(c, y_position, height)

        # 2. Расширенная статистика
        self._add_section_header(c, "2. РАСШИРЕННАЯ СТАТИСТИКА", y_position)
        y_position = self._add_statistics_section(c, analysis_results, y_position - 10 * mm)
        y_position = self._check_and_add_page(c, y_position, height)

        # 3. Метрики ML-модели
        self._add_section_header(c, "3. МЕТРИКИ МОДЕЛИ", y_position)
        y_position = self._add_ml_metrics_section(c, analysis_results, y_position - 10 * mm)
        y_position = self._check_and_add_page(c, y_position, height)

        # 4. Визуализации
        self._add_section_header(c, "4. ВИЗУАЛИЗАЦИИ", y_position)
        self._add_visualizations_section(c, analysis_results, df, y_position - 10 * mm, width, height)

        c.save()
        logger.info(f"PDF-отчёт сохранён как {output_path}")
        return output_path

    def _check_and_add_page(self, c: canvas.Canvas, y_position: float, page_height: float) -> float:
        """Проверяет, нужно ли перейти на новую страницу, и возвращает новую позицию Y"""
        if y_position < 40 * mm: # Увеличено минимальное пространство
            c.showPage()
            c.setFont(self.default_font, 12) # Сброс шрифта после смены страницы
            return page_height - 30 * mm
        return y_position

    def _add_section_header(self, c: canvas.Canvas, title: str, y_position: float):
        """Добавляет заголовок раздела."""
        c.setFont(self.default_font, 14)
        c.drawString(30 * mm, y_position, title)
        c.setFont(self.default_font, 10) # Возвращаем основной шрифт

    def _add_summary_section(self, c: canvas.Canvas, analysis_results: Dict[str, Any], y_position: float) -> float:
        """Добавляет раздел сводки анализа."""
        summary = analysis_results.get('summary', {}) # summary теперь передается в analysis_results
        
        current_y = y_position
        for key, value in summary.items():
            text_line = f"• {key.replace('_', ' ').capitalize()}: {value}"
            c.drawString(30 * mm, current_y, text_line)
            current_y -= 8 * mm
            if current_y < 40 * mm: # Проверка на переполнение страницы
                c.showPage()
                c.setFont(self.default_font, 10)
                current_y = A4[1] - 30 * mm
        return current_y

    def _add_statistics_section(self, c: canvas.Canvas, analysis_results: Dict[str, Any], y_position: float) -> float:
        """Добавляет раздел расширенной статистики"""
        stats_data = analysis_results.get('statistics', {}) # Получаем корневой словарь
        extended_stats = stats_data.get('extended', {}) # И уже из него extended
        
        current_y = y_position
        if not extended_stats:
            c.drawString(30 * mm, current_y, "Расширенная статистика отсутствует.")
            return current_y - 8 * mm

        for col, stats in extended_stats.items():
            current_y -= 8 * mm
            if current_y < 50 * mm: # Проверка на переполнение
                c.showPage()
                c.setFont(self.default_font, 10)
                current_y = A4[1] - 30 * mm
                self._add_section_header(c, "2. РАСШИРЕННАЯ СТАТИСТИКА (продолжение)", current_y)
                current_y -= 10 * mm

            c.drawString(30 * mm, current_y, f"Столбец: {col}")
            current_y -= 6 * mm
            for stat_name, value in stats.items():
                if isinstance(value, (float, np.float64)):
                    text_line = f"  {stat_name}: {value:.4f}"
                else:
                    text_line = f"  {stat_name}: {value}"
                c.drawString(40 * mm, current_y, text_line)
                current_y -= 6 * mm
        return current_y


    def _add_ml_metrics_section(self, c: canvas.Canvas, analysis_results: Dict[str, Any], y_position: float) -> float:
        """Добавляет раздел метрик ML-модели"""
        ml_model_data = analysis_results.get('ml_model', {})
        metrics = ml_model_data.get('metrics', {})
        
        current_y = y_position
        if not metrics:
            c.drawString(30 * mm, current_y, "Метрики ML-модели отсутствуют или модель не обучена.")
            return current_y - 8 * mm

        c.drawString(30 * mm, current_y, f"Тип модели: {ml_model_data.get('model_type', 'N/A')}")
        current_y -= 8 * mm

        for metric, value in metrics.items():
            if isinstance(value, (float, np.float64)):
                text_line = f"  {metric}: {value:.4f}"
            else:
                text_line = f"  {metric}: {value}"
            c.drawString(40 * mm, current_y, text_line)
            current_y -= 6 * mm
        return current_y

    def _add_visualizations_section(self, c: canvas.Canvas, analysis_results: Dict[str, Any], df: Optional[pd.DataFrame], y_position: float, width: float, height: float):
        """Добавляет раздел визуализаций"""
        temp_dir = tempfile.mkdtemp()
        figures_paths: List[str] = []

        try:
            # 1. Корреляционная матрица
            corr_data = analysis_results.get('correlations', {})
            if corr_data:
                corr_matrix = pd.DataFrame.from_dict(corr_data)
                if not corr_matrix.empty:
                    corr_fig = self._plot_correlation_matrix(corr_matrix)

                    # Проверяем, что функция вернула фигуру, а не None
                    if corr_fig is not None:
                        img_path = os.path.join(temp_dir, 'correlation_matrix.png')
                        corr_fig.savefig(img_path, dpi=150, bbox_inches='tight')
                        figures_paths.append(img_path)
                        plt.close(corr_fig)
                    else:
                        logger.warning("Не удалось создать визуализацию корреляционной матрицы: функция _plot_correlation_matrix вернула None.")
                else:
                    logger.warning("Корреляционная матрица пуста, пропуск визуализации.")
        except Exception as e:
            logger.error(f"Критическая ошибка при обработке корреляционной матрицы: {e}")

            # 2. Гистограммы
            if df is not None and not df.empty:
                hist_fig = self._plot_histograms(df)
                if hist_fig is not None:
                    img_path = os.path.join(temp_dir, 'histograms.png')
                    hist_fig.savefig(img_path, dpi=150, bbox_inches='tight')
                    figures_paths.append(img_path)
                    plt.close(hist_fig)
                else:
                    logger.warning("Не удалось построить гистограммы, пропуск визуализации.")
            else:
                logger.warning("DataFrame для гистограмм отсутствует или пуст, пропуск визуализации.")

            # Добавляем графики в PDF
            current_y = y_position
            for img_path in figures_paths:
                if os.path.exists(img_path):
                    current_y -= 10 * mm # Отступ перед картинкой
                    if current_y < 80 * mm: # Проверка на переполнение страницы
                        c.showPage()
                        c.setFont(self.default_font, 10)
                        current_y = height - 30 * mm

                    # Вычисляем пропорции для вставки изображения
                    img_width = 150 * mm
                    img_height = 0 # Будет рассчитано
                    
                    try:
                        from PIL import Image
                        with Image.open(img_path) as img:
                            ratio = img.height / img.width
                            img_height = img_width * ratio
                    except ImportError:
                        logger.warning("Pillow не установлен, не могу определить размер изображения. Использую фиксированный.")
                        img_height = 100 * mm # Фиксированная высота, если Pillow нет

                    c.drawImage(img_path, 30 * mm, current_y - img_height,
                                width=img_width, height=img_height, preserveAspectRatio=True)
                    current_y -= (img_height + 10 * mm) # Обновляем позицию Y после картинки
                else:
                    logger.warning(f"Изображение по пути {img_path} не найдено, пропуск.")
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logger.debug(f"Временная директория {temp_dir} удалена.")

    def _plot_correlation_matrix(self, corr_matrix: pd.DataFrame) -> Figure | None:
        """Построение корреляционной матрицы. Улучшена устойчивость."""
        # Фильтруем только числовые колонки, исключая Timedelta и другие неподходящие типы
        numeric_cols = []
        for col in corr_matrix.columns:
            col_data = corr_matrix[col]
            if (pd.api.types.is_numeric_dtype(col_data) and
                    not pd.api.types.is_timedelta64_dtype(col_data) and
                    not pd.api.types.is_object_dtype(col_data)):
                numeric_cols.append(col)

        corr_numeric = corr_matrix[numeric_cols].astype(float, errors='ignore')

        if corr_numeric.empty:
            logger.warning("Корреляционная матрица после фильтрации не содержит числовых данных (исключены Timedelta, object и др.).")
            return None

        # Удаляем столбцы/строки, если все значения NaN
        corr_numeric = corr_numeric.dropna(axis=1, how='all').dropna(axis=0, how='all')
        if corr_numeric.empty:
            logger.warning("После удаления NaN корреляционная матрица пуста.")
            return None

        # Проверяем достаточное количество колонок для корреляции
        if corr_numeric.shape[1] < 2:
            logger.warning(f"Недостаточно числовых колонок для вычисления корреляции: {corr_numeric.shape[1]} (требуется ≥2).")
            return None

        # Вычисляем корреляцию
        try:
            corr_computed = corr_numeric.corr()
        except Exception as e:
            logger.error(f"Ошибка вычисления корреляции: {e}")
            return None

        # Создаём фигуру и оси
        fig: Figure = plt.figure(figsize=(10, 8))
        ax = fig.add_subplot(111)

        # Строим тепловую карту
        im = ax.imshow(
            corr_computed,
            cmap='coolwarm',
            vmin=-1,
            vmax=1,
            aspect='auto'
        )

        # Настраиваем подписи осей
        ax.set_xticks(range(len(corr_computed.columns)))
        ax.set_yticks(range(len(corr_computed.index)))
        ax.set_xticklabels(corr_computed.columns, rotation=45, ha='right')
        ax.set_yticklabels(corr_computed.index)
        ax.set_title('Корреляционная матрица', fontsize=14, pad=20)

        # Добавляем значения корреляции в ячейки с полной обработкой ошибок
        for i in range(len(corr_computed.columns)):
            for j in range(len(corr_computed.index)):
                try:
                    corr_value = corr_computed.iloc[i, j]

                    # Проверяем, что значение — число и не NaN
                    if pd.isna(corr_value) or not isinstance(corr_value, (int, float)):
                        text = "N/A"
                        text_color = "gray"
                    else:
                        # Безопасное вычисление abs
                        try:
                            abs_value = abs(corr_value)
                            text_color = "white" if abs_value > 0.5 else "black"
                            text = f"{corr_value:.2f}"
                        except (TypeError, ValueError):
                            text = "ERR"
                            text_color = "red"

                    ax.text(
                        j, i,
                        text,
                        ha="center", va="center",
                        color=text_color,
                        fontsize=8
                    )
                except Exception as cell_error:
                    logger.debug(f"Ошибка обработки ячейки ({i},{j}): {cell_error}")
                    # В случае любой ошибки — выводим заглушку
                    ax.text(j, i, "?", ha="center", va="center", color="red", fontsize=8)

        # Цветная шкала
        plt.colorbar(im, ax=ax, label='Коэффициент корреляции')

        # Улучшаем компоновку
        plt.tight_layout()

        return fig

    def _plot_histograms(self, df: pd.DataFrame) -> Figure | None:
        """Построение гистограмм для числовых столбцов. Динамическое количество сабплотов."""
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) == 0:
            logger.warning("Нет числовых столбцов для построения гистограмм.")
            return None

        # Ограничиваем количество гистограмм для PDF, чтобы не перегружать отчёт
        cols_to_plot = numeric_cols[:min(len(numeric_cols), 6)]  # Максимум 6 гистограмм
        n_plots = len(cols_to_plot)
        if n_plots == 0:
            return None

        n_rows = (n_plots + 1) // 2 if n_plots > 1 else 1  # Минимум 1 строка, если 1 график
        n_cols = 2 if n_plots > 1 else 1

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(5 * n_cols, 4 * n_rows))
        axes = axes.flatten() if n_plots > 1 else [axes]  # Обрабатываем случай с одним графиком

        for i, col in enumerate(cols_to_plot):
            # Передаём DataFrame с одним столбцом вместо Series
            data_to_plot = df[[col]].dropna()
            sns.histplot(
                data=data_to_plot,
                x=col,
                kde=True,
                ax=axes[i],
                color='skyblue',
                edgecolor='black'
            )
            axes[i].set_title(f'Распределение: {col}')
            axes[i].set_xlabel('Значение')
            axes[i].set_ylabel('Частота')

        # Скрываем неиспользуемые подграфики
        for j in range(n_plots, len(axes)):
            fig.delaxes(axes[j])

        plt.tight_layout()
        return fig

class ExcelReportGenerator:
    def export_to_excel(
            self,
            df: pd.DataFrame,
            analysis_results: Dict[str, Any],
            output_path: str
        ) -> str:
        try:
            # Явно указываем движок openpyxl для поддержки column_dimensions
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # 1. Исходные данные
                if df is not None and not df.empty:
                    df.to_excel(writer, sheet_name='Исходные_данные', index=False)
                    logger.info("Лист 'Исходные_данные' добавлен.")

                # 2. Статистика
                self._add_statistics_sheet(writer, analysis_results)
                # 3. Корреляции
                self._add_correlations_sheet(writer, analysis_results)
                # 4. Метрики ML
                self._add_ml_metrics_sheet(writer, analysis_results)
                # 5. Сводка
                self._add_summary_sheet(writer, df, analysis_results)

            logger.info(f"Excel-отчёт сохранён как {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Критическая ошибка при создании Excel-отчёта: {e}", exc_info=True)
            raise

    def _apply_header_style(self, worksheet, row_num: int, col_num: int):
        """Применяет стиль заголовка к ячейке."""
        cell = worksheet.cell(row=row_num, column=col_num)
        cell.font = Font(bold=True, color='FFFFFF')
        cell.fill = PatternFill(start_color='2E74B5', end_color='2E74B5', fill_type='solid')
        cell.alignment = Alignment(horizontal='center', vertical='center')

    def _add_statistics_sheet(self, writer: pd.ExcelWriter, analysis_results: Dict[str, Any]):
        """Добавляет лист со статистикой в Excel-файл."""
        stats_data = analysis_results.get('statistics', {}).get('extended', {})

        if not stats_data:
            logger.warning("Данные расширенной статистики отсутствуют. Лист 'Статистика' не будет создан.")
            return

        stats_df = pd.DataFrame(stats_data).T.round(4)  # Округляем для читаемости

        if stats_df.empty:
            logger.warning("DataFrame статистики пуст. Лист 'Статистика' не будет создан.")
            return

        sheet_name = 'Статистика'
        stats_df.to_excel(writer, sheet_name=sheet_name, index=True)
        worksheet = writer.sheets[sheet_name]

        # Автоподбор ширины колонок
        for i, col in enumerate(stats_df.columns):
            # Вычисляем максимальную длину в колонке (учитываем заголовок)
            max_len = max(
                stats_df[col].astype(str).map(len).max(),
                len(str(col))
            ) + 2  # Добавляем отступ
            # Ограничиваем максимальную ширину (например, 50 символов)
            adjusted_width = min(max_len, 50)
            worksheet.column_dimensions[get_column_letter(i + 2)].width = adjusted_width

        # Ширина для индексной колонки (имен признаков)
        if not stats_df.index.empty:
            max_idx_len = len(str(stats_df.index[-1])) + 2
            worksheet.column_dimensions['A'].width = min(max_idx_len, 20)  # Колонка A — индексы

        # Форматирование заголовков
        for col_idx in range(1, len(stats_df.columns) + 2):  # +1 для индексной колонки
            self._apply_header_style(worksheet, 1, col_idx)

        logger.info(f"Лист '{sheet_name}' успешно добавлен и отформатирован.")

    def _add_correlations_sheet(self, writer: pd.ExcelWriter, analysis_results: Dict[str, Any]):
        """Добавляет лист с корреляционной матрицей в Excel-файл."""
        corr_data = analysis_results.get('correlations', {})

        if not corr_data:
            logger.warning("Корреляционная матрица отсутствует. Лист 'Корреляции' не будет создан.")
            return
        
        # Преобразуем словарь обратно в DataFrame
        corr_matrix = pd.DataFrame.from_dict(corr_data)

        if not isinstance(corr_matrix, pd.DataFrame) or corr_matrix.empty:
            logger.warning("Корреляционная матрица пуста или имеет неверный формат. Лист 'Корреляции' не будет создан.")
            return

        sheet_name = 'Корреляции'
        corr_matrix.round(2).to_excel(writer, sheet_name=sheet_name, index=True) # Округляем до 2 знаков
        worksheet = writer.sheets[sheet_name]

        # Автоширина для колонок
        for i, col in enumerate(corr_matrix.columns):
            max_len = max(corr_matrix[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.column_dimensions[get_column_letter(i + 2)].width = max_len
        # Ширина для индексной колонки (имен признаков)
        max_idx_len = max(corr_matrix.index.astype(str).map(len).max(), len('Признак')) + 2
        worksheet.column_dimensions[get_column_letter(1)].width = max_idx_len

        # Форматирование заголовков
        for col_idx in range(1, len(corr_matrix.columns) + 2):
            self._apply_header_style(worksheet, 1, col_idx)

        # Центрирование содержимого
        for row in worksheet.iter_rows(min_row=2, min_col=2):
            for cell in row:
                cell.alignment = Alignment(horizontal='center', vertical='center')

        logger.info(f"Лист '{sheet_name}' успешно добавлен и отформатирован.")


    def _add_ml_metrics_sheet(self, writer: pd.ExcelWriter, analysis_results: Dict[str, Any]):
        """Добавляет лист с метриками ML-модели"""
        ml_model_data = analysis_results.get('ml_model', {})
        metrics = ml_model_data.get('metrics', {})

        if not metrics:
            logger.warning("Метрики ML-модели отсутствуют. Лист 'ML_метрики' не будет создан.")
            return

        metrics_df = pd.DataFrame([metrics]).round(4) # Округляем для читаемости
        sheet_name = 'ML_метрики'
        metrics_df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]

        # Автоширина
        for i, col in enumerate(metrics_df.columns):
            max_len = max(metrics_df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.column_dimensions[get_column_letter(i + 1)].width = max_len

        # Форматирование заголовков
        for col_idx in range(1, len(metrics_df.columns) + 1):
            self._apply_header_style(worksheet, 1, col_idx)

        logger.info(f"Лист '{sheet_name}' успешно добавлен и отформатирован.")


    def _add_summary_sheet(self, writer: pd.ExcelWriter, df: pd.DataFrame, analysis_results: Dict[str, Any]):
        """Добавляет сводный лист."""
        # Используем существующий summary из analysis_results, если он есть
        summary_data_from_analyzer = analysis_results.get('summary', {})

        summary_rows: List[Dict[str, Any]] = []

        # Базовая информация
        summary_rows.append({'Параметр': 'Общее количество записей', 'Значение': len(df)})
        summary_rows.append({'Параметр': 'Количество столбцов', 'Значение': len(df.columns)})
        summary_rows.append({'Параметр': 'Дата генерации отчёта', 'Значение': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        
        # Добавляем информацию из summary, если она не перекрывается
        for key, value in summary_data_from_analyzer.items():
            # Избегаем дублирования базовых полей
            if key not in ['total_rows', 'total_columns', 'generated_at']:
                summary_rows.append({'Параметр': key.replace('_', ' ').capitalize(), 'Значение': value})

        # Добавляем метрики ML, если доступны
        ml_model_data = analysis_results.get('ml_model', {})
        metrics = ml_model_data.get('metrics', {})
        if metrics:
            summary_rows.append({'Параметр': '--- Метрики ML-модели ---', 'Значение': ''})
            summary_rows.append({'Параметр': 'Тип модели', 'Значение': ml_model_data.get('model_type', 'N/A')})
            for metric, value in metrics.items():
                if isinstance(value, (float, np.float64)):
                    summary_rows.append({'Параметр': metric.capitalize(), 'Значение': f"{value:.4f}"})
                else:
                    summary_rows.append({'Параметр': metric.capitalize(), 'Значение': value})

        summary_df = pd.DataFrame(summary_rows)
        sheet_name = 'Сводка'
        summary_df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]

        # Автоширина колонок
        for col_idx in range(1, len(summary_df.columns) + 1):
            max_len = max(summary_df.iloc[:, col_idx - 1].astype(str).map(len).max(), len(summary_df.columns[col_idx - 1])) + 2
            worksheet.column_dimensions[get_column_letter(col_idx)].width = max_len

        # Форматирование заголовка
        self._apply_header_style(worksheet, 1, 1)
        self._apply_header_style(worksheet, 1, 2)
        
        # Жирный шрифт для "Параметр"
        for row_idx in range(2, worksheet.max_row + 1):
            worksheet.cell(row=row_idx, column=1).font = Font(bold=True)

        logger.info(f"Лист '{sheet_name}' успешно добавлен и отформатирован.")


class EmailSender:
    def __init__(self, smtp_config: Dict[str, Any]):
        self.smtp_config = smtp_config
        self._validate_smtp_config()

    def _validate_smtp_config(self):
        """Проверяет наличие необходимых полей в конфигурации SMTP."""
        required_keys = ['server', 'port', 'email', 'password']
        for key in required_keys:
            if key not in self.smtp_config or not self.smtp_config[key]:
                raise ValueError(f"Недостающая или пустая настройка SMTP: '{key}'")

    def send_email(
        self,
        subject: str,
        body: str,
        to_emails: List[str],
        attachments: Optional[List[str]] = None
    ) -> bool: 
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config['email']
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain', 'utf-8')) # Указываем кодировку UTF-8

            if attachments:
                for file_path in attachments:
                    if os.path.exists(file_path):
                        with open(file_path, "rb") as attachment_file:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment_file.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {os.path.basename(file_path)}'
                            )
                            msg.attach(part)
                        logger.info(f"Вложение '{os.path.basename(file_path)}' добавлено.")
                    else:
                        logger.warning(f"Файл '{file_path}' не найден для вложения. Пропуск.")

            # Отправляем письмо ОДИН РАЗ после обработки всех вложений
            server = smtplib.SMTP(self.smtp_config['server'], self.smtp_config['port'])
            server.starttls() # Включаем TLS для безопасного соединения
            server.login(self.smtp_config['email'], self.smtp_config['password'])
            text = msg.as_string()
            server.sendmail(self.smtp_config['email'], to_emails, text)
            server.quit()
            logger.info(f"Письмо отправлено на: {', '.join(to_emails)}")
            return True

        except smtplib.SMTPAuthenticationError:
            logger.error("Ошибка аутентификации SMTP. Проверьте логин/пароль или настройки приложения.")
            return False
        except smtplib.SMTPConnectError as e:
            logger.error(f"Ошибка подключения SMTP: {e}. Проверьте сервер и порт.")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке email: {e}", exc_info=True)
            return False

class ReportManager:
    """
    Основной класс для управления созданием отчётов разных форматов
    Объединяет функциональность PDF, Excel и email отправки
    """
    def __init__(self, smtp_config: Optional[Dict[str, Any]] = None):
        self.pdf_generator = PDFReportGenerator()
        self.excel_generator = ExcelReportGenerator()

        if smtp_config:
            try:
                self.email_sender: Optional[EmailSender] = EmailSender(smtp_config)
                logger.info("Email-отправка настроена успешно.")
            except ValueError as e:
                self.email_sender = None
                logger.error(f"Ошибка конфигурации SMTP: {e}. Email-отправка недоступна.")
            except Exception as e:
                self.email_sender = None
                logger.error(f"Неожиданная ошибка при инициализации EmailSender: {e}. Email-отправка недоступна.")
        else:
            self.email_sender = None
            logger.warning("Email-отправка не настроена: отсутствует конфигурация SMTP.")

    def generate_reports(
        self,
        df: pd.DataFrame,
        analysis_results: Dict[str, Any],
        output_dir: str,
        output_format: str = "pdf",  
        send_email: bool = False,
        email_recipients: Optional[List[str]] = None
    ) -> Dict[str, str]:
        """
        Генерирует отчёты в указанном формате (PDF, Excel или оба)

        Args:
            df: исходный DataFrame с данными
            analysis_results: результаты анализа данных
            output_dir: директория для сохранения отчётов
            output_format: формат отчёта ('pdf', 'excel', 'both')
            send_email: флаг отправки отчётов по email
            email_recipients: список получателей email

        Returns:
            Словарь с путями к созданным файлам отчётов
        """
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        result_paths: Dict[str, str] = {}

        # Добавляем сводную информацию в analysis_results для консистентности
        analysis_results['summary'] = self._create_report_summary(df, analysis_results)

        # Валидация формата
        if output_format not in ['pdf', 'excel', 'both']:
            raise ValueError(f"Неподдерживаемый формат отчёта: {output_format}. "
                            f"Допустимые значения: 'pdf', 'excel', 'both'")

        # Генерируем PDF отчёт, если требуется
        if output_format in ['pdf', 'both']:
            pdf_path = os.path.join(output_dir, f'report_{timestamp}.pdf')
            try:
                pdf_path_actual = self.pdf_generator.create_pdf_report(analysis_results, df, pdf_path)
                if pdf_path_actual:
                    result_paths['pdf'] = pdf_path_actual
            except ValidationError as e:
                logger.error(f"Ошибка валидации данных для PDF отчёта: {e}. PDF не создан.")
            except Exception as e:
                logger.error(f"Неожиданная ошибка при создании PDF отчёта: {e}", exc_info=True)

        # Генерируем Excel отчёт, если требуется
        if output_format in ['excel', 'both']:
            excel_path = os.path.join(output_dir, f'report_{timestamp}.xlsx')
            try:
                excel_path_actual = self.excel_generator.export_to_excel(df, analysis_results, excel_path)
                if excel_path_actual:
                    result_paths['excel'] = excel_path_actual
            except Exception as e:
                logger.error(f"Ошибка при создании Excel отчёта: {e}", exc_info=True)

        # Отправка по email, если требуется и есть отчёты для отправки
        if send_email and result_paths:
            if not email_recipients:
                logger.warning("Не указаны получатели email. Отправка отменена.")
            elif not self.email_sender:
                logger.warning("EmailSender не настроен. Отправка невозможна.")
            else:
                self._send_reports_via_email(result_paths, email_recipients)

        return result_paths

    def _create_report_summary(self, df: pd.DataFrame, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Создает сводную информацию для отчета."""
        summary = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'numeric_columns': len(df.select_dtypes(include=np.number).columns),
            'categorical_columns': len(df.select_dtypes(exclude=np.number).columns),
            'missing_values_total': int(df.isnull().sum().sum()),
            'ml_model_status': 'N/A',
            'ml_model_metrics': {}
        }
        
        ml_model_data = analysis_results.get('ml_model', {})
        if ml_model_data.get('success'):
            summary['ml_model_status'] = f"Успешно обучена ({ml_model_data.get('model_type', 'N/A')})"
            metrics = {k: f"{v:.4f}" if isinstance(v, (float, np.float64)) else v for k, v in ml_model_data.get('metrics', {}).items()}
            summary['ml_model_metrics'] = metrics
        elif ml_model_data.get('error'):
            summary['ml_model_status'] = f"Ошибка: {ml_model_data['error']}"
        else:
            summary['ml_model_status'] = "Модель не обучалась."

        return summary


    def _send_reports_via_email(
        self,
        report_paths: Dict[str, str],
        recipients: List[str]
    ) -> bool:
        """Отправляет отчёты по email."""
        if self.email_sender is None:
            logger.warning("EmailSender не инициализирован. Отправка email невозможна.")
            return False

        subject = f"Отчёт анализа данных от {datetime.now().strftime('%Y-%m-%d')}"
        body = "Здравствуйте,\n\nВо вложении представлены результаты автоматического анализа данных в форматах PDF и Excel.\n\nС уважением,\nСистема анализа данных."
        attachments = list(report_paths.values())

        success = self.email_sender.send_email(subject, body, recipients, attachments)
        if success:
            logger.info("Отчёты успешно отправлены по email.")
            return True
        else:
            logger.error("Не удалось отправить отчёты по email.")
            return False

