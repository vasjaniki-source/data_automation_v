
# report_manager.py
import logging
import math
import numbers
import mimetypes
import concurrent.futures
from typing import Dict, Any, Optional, List, Callable
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import rcParams
import matplotlib.pyplot as plt
from PIL import Image as PILImage

# Excel
import openpyxl

# Email / SMTP
import smtplib
from email.message import EmailMessage

logger = logging.getLogger(__name__)
rcParams.update({"font.size": 10, "font.family": "sans-serif", "figure.dpi": 150})

# Вспомогательная функция (как у вас ранее) - рекурсивное приведение к нативным типам
def _ensure_native(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    if isinstance(obj, (pd.Series, pd.Index, np.ndarray)):
        return [_ensure_native(x) for x in obj.tolist()]
    if isinstance(obj, dict):
        return {str(k) if k is not None else None: _ensure_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_ensure_native(x) for x in obj]
    if isinstance(obj, np.generic):
        try:
            return obj.item()
        except Exception:
            return str(obj)
    if isinstance(obj, pd.Timestamp):
        return obj.to_pydatetime()
    if isinstance(obj, numbers.Real):
        try:
            if math.isnan(obj) or math.isinf(obj):
                return None
        except Exception:
            pass
    if isinstance(obj, (bool, np.bool_)):
        return bool(obj)
    if isinstance(obj, numbers.Integral):
        return int(obj)
    if isinstance(obj, numbers.Real):
        try:
            return float(obj)
        except Exception:
            return None
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    return str(obj)


# PDF generator (сокращённо, базовая функциональность)
class PDFReportGenerator:
    def __init__(self, output_path: str, log_callback: Optional[Callable[[str, str], None]] = None,
                 rows_per_page: int = 20, a4_landscape: bool = True):
        self.output_path = output_path
        self.log_callback = log_callback
        self.rows_per_page = max(5, int(rows_per_page))
        self.a4_landscape = a4_landscape

    def _log(self, msg: str, level: str = "info"):
        if self.log_callback:
            self.log_callback(msg, level.upper())
        logger.log(getattr(logging, level.upper(), logging.INFO), msg)

    def _format_cell(self, x):
        if pd.isna(x):
            return ""
        if isinstance(x, float):
            return f"{x:,.2f}"
        return str(x)

    def create_pdf(self, analysis_results: Dict[str, Any], df: Optional[pd.DataFrame] = None, images: Optional[List[str]] = None):
        out_path = Path(self.output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        title_text = "Аналитический отчет"
        date_text = datetime.now().strftime('%d.%m.%Y %H:%M')

        cols = []
        table_rows_all = []
        if df is not None and not df.empty:
            cols = list(df.columns)
            for i in range(len(df)):
                row = [self._format_cell(df.iloc[i].get(c)) for c in cols]
                table_rows_all.append(row)

        try:
            with PdfPages(str(out_path)) as pdf:
                if table_rows_all:
                    num_pages = math.ceil(len(table_rows_all) / self.rows_per_page)
                    for p in range(num_pages):
                        fig = plt.figure(figsize=(11.69, 8.27) if self.a4_landscape else (8.27, 11.69))
                        fig.suptitle(title_text, fontsize=14, fontweight='bold')
                        ax_table = fig.add_axes((0.03, 0.25, 0.94, 0.65))
                        ax_table.axis('off')
                        start = p * self.rows_per_page
                        end = min(len(table_rows_all), start + self.rows_per_page)
                        subrows = table_rows_all[start:end]
                        table = ax_table.table(cellText=subrows, colLabels=cols, loc='upper left')
                        table.auto_set_font_size(False); table.set_fontsize(9)
                        pdf.savefig(fig); plt.close(fig)
                else:
                    fig = plt.figure(figsize=(11.69, 8.27) if self.a4_landscape else (8.27, 11.69))
                    fig.suptitle(title_text, fontsize=14, fontweight='bold')
                    fig.text(0.01, 0.94, date_text, fontsize=9)
                    pdf.savefig(fig); plt.close(fig)

                # images pages
                if images:
                    for img_path in images:
                        try:
                            img = PILImage.open(img_path)
                        except Exception as e:
                            self._log(f"Не удалось открыть изображение {img_path}: {e}", "warning")
                            continue
                        fig = plt.figure(figsize=(11.69, 8.27) if self.a4_landscape else (8.27, 11.69))
                        fig.suptitle(title_text, fontsize=14, fontweight='bold')
                        ax = fig.add_axes((0.05, 0.07, 0.9, 0.82))
                        ax.axis('off')
                        ax.imshow(img)
                        pdf.savefig(fig); plt.close(fig)

            self._log(f"PDF успешно создан: {out_path}", "info")
            return str(out_path), images or []
        except Exception as e:
            self._log(f"Ошибка при создании PDF: {e}", "error")
            raise


# Excel generator
class ExcelReportGenerator:
    def __init__(self, output_path: str, log_callback: Optional[Callable[[str, str], None]] = None):
        self.output_path = output_path
        self.log_callback = log_callback

    def _log(self, msg: str, level: str = "info"):
        if self.log_callback:
            self.log_callback(msg, level.upper())
        logger.log(getattr(logging, level.upper(), logging.INFO), msg)
        
    def export_to_excel(
        self,
        df,
        data: pd.DataFrame,
        analysis_results: Dict[str, Any],
        output_path: str
    ) -> str:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            try:
                # 1. Исходные данные
                if data is not None and isinstance(data, pd.DataFrame) and not data.empty:
                    data.to_excel(writer, sheet_name='Исходные_данные', index=False)
                    logger.info("Лист 'Исходные_данные' добавлен в Excel")
                else:
                    logger.warning(
                        "Пропуск листа 'Исходные_данные': "
                        f"data={type(data) if data is not None else 'None'}, "
                        f"пустой={data.empty if isinstance(data, pd.DataFrame) else 'N/A'}"
                    )

                # Проверка наличия analysis_results один раз
                analysis_available = analysis_results and isinstance(analysis_results, dict)

                # 2. Расширенная статистика
                if analysis_available:
                    self._add_statistics_sheet(writer, analysis_results)
                else:
                    self._create_error_sheet(writer, 'Расширенная статистика', 'Отсутствуют или некорректны analysis_results')

                # 3. Корреляционная матрица
                if analysis_available:
                    self._add_correlations_sheet(writer, analysis_results)
                else:
                    self._create_error_sheet(writer, 'Корреляционная матрица', 'Отсутствуют или некорректны analysis_results')

                # 4. Метрики ML-модели
                if analysis_available:
                    self._add_ml_metrics_sheet(writer, analysis_results)
                else:
                    self._create_error_sheet(writer, 'Метрики ML-модели', 'Отсутствуют или некорректны analysis_results')

                # 5. Выбросы
                if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                    logger.warning(
                        "Пропуск создания листа 'Выбросы': "
                        f"df={type(df) if df is not None else 'None'}, "
                f"пустой={df.empty if isinstance(df, pd.DataFrame) else 'N/A'}"
            )
                else:
                    self._add_outliers_sheet(writer, analysis_results, df)

                # 6. Анализ временного ряда
                if analysis_available and 'trend_analysis' in analysis_results:
                    try:
                        self._add_time_series_sheet(writer, analysis_results)
                    except Exception as e:
                        logger.error(f"Ошибка при добавлении листа 'Анализ временного ряда': {e}")
                        self._create_error_sheet(
                            writer,
                            'Временные ряды',
                            f'Ошибка создания листа: {str(e)}\nДанные о тренде недоступны'
                        )
                else:
                    self._create_error_sheet(
                        writer,
                        'Временные ряды',
                        'Анализ временных рядов недоступен: отсутствуют данные trend_analysis в analysis_results'
                    )

                # 7. Общая сводка анализа
                if (data is not None and isinstance(data, pd.DataFrame)) and analysis_available:
                    self._add_summary_sheet(writer, data, analysis_results)
                else:
                    self._create_error_sheet(
                        writer,
                        'Сводка',
                        'Сводка анализа недоступна: недостаточные данные для построения сводки'
                    )

                logger.info(f"Excel-отчёт успешно создан: {output_path}")

            except Exception as e:
                logger.error(f"Критическая ошибка при создании Excel-отчёта: {e}")
                raise

        logger.info(f"Excel-отчёт успешно сохранён как {output_path}")
        return output_path

    def _add_statistics_sheet(self, writer, analysis_results):
        """Добавляет лист со статистикой с полной обработкой ошибок и проверок"""
        try:
            # Проверка входных данных
            if not analysis_results:
                logger.warning("Пропуск листа 'Статистика': analysis_results отсутствует")
                return

            if not isinstance(analysis_results, dict):
                logger.warning(
                    f"Пропуск листа 'Статистика': analysis_results имеет неверный тип {type(analysis_results)}"
                )
                return

            if 'statistics' not in analysis_results:
                logger.warning("Пропуск листа 'Статистика': отсутствует раздел 'statistics' в analysis_results")
                return

            stats_section = analysis_results['statistics']

            if not isinstance(stats_section, dict):
                logger.warning(
                    f"Пропуск листа 'Статистика': раздел 'statistics' имеет неверный тип {type(stats_section)}"
                )
                return

            if 'extended' not in stats_section:
                logger.warning("Пропуск листа 'Статистика': отсутствуют расширенные данные ('extended')")
                return

            stats_data = stats_section['extended']

            if not stats_data:
                logger.warning("Пропуск листа 'Статистика': расширенные данные пусты")
                return

            if not isinstance(stats_data, dict):
                logger.warning(
                    f"Пропуск листа 'Статистика': расширенные данные имеют неверный тип {type(stats_data)}"
                )
                return

            # Дополнительная проверка: есть ли данные для отображения
            if not any(stats_data.values()):
                logger.warning("Пропуск листа 'Статистика': нет данных для отображения в расширенной статистике")
                return

            # Преобразование в DataFrame
            try:
                stats_df = pd.DataFrame(stats_data).T

                # Проверка, что DataFrame не пустой
                if stats_df.empty:
                    logger.warning("Пропуск листа 'Статистика': полученный DataFrame пуст")
                    return

                # Запись в Excel
                stats_df.to_excel(writer, sheet_name='Статистика', index=True)
                logger.info(f"Лист 'Статистика' добавлен в Excel. Обработано столбцов: {len(stats_df)}")

            except ValueError as e:
                logger.error(f"Ошибка преобразования данных в DataFrame: {e}")
                self._create_error_sheet(writer, 'Статистика', f"Ошибка преобразования данных: {str(e)}")
            except Exception as e:
                logger.error(f"Неожиданная ошибка при создании листа 'Статистика': {e}")
                self._create_error_sheet(writer, 'Статистика', f"Общая ошибка: {str(e)}")

        except Exception as e:
            logger.critical(f"Критическая ошибка в методе _add_statistics_sheet: {e}")
            self._create_error_sheet(writer, 'Статистика', f"Критическая ошибка метода: {str(e)}")


    def _create_error_sheet(self, writer, sheet_name: str, error_message: str):
        try:
            error_df = pd.DataFrame({
                'Ошибка при создании листа': [error_message],
                'Время ошибки': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            error_df.to_excel(writer, sheet_name=sheet_name, index=False)
            logger.warning(f"Создан лист-заглушка '{sheet_name}' с сообщением об ошибке")
        except Exception as e:
            logger.error(f"Не удалось создать лист-заглушку '{sheet_name}': {e}")

    def _add_correlations_sheet(self, writer, analysis_results):
        """Добавляет лист с корреляционной матрицей с обработкой ошибок"""
        try:
            # Проверка входных данных
            if not analysis_results or not isinstance(analysis_results, dict):
                logger.warning("Пропуск листа 'Корреляции': analysis_results отсутствует или некорректен")
                return

            if 'statistics' not in analysis_results:
                logger.warning("Пропуск листа 'Корреляции': отсутствует раздел 'statistics'")
                return

            stats_data = analysis_results['statistics']
            if 'correlation_analysis' not in stats_data:
                logger.warning("Пропуск листа 'Корреляции': отсутствует анализ корреляций")
                return

            corr_analysis = stats_data['correlation_analysis']
            if 'correlation_matrix' not in corr_analysis:
                logger.warning("Пропуск листа 'Корреляции': отсутствует корреляционная матрица")
                return

            corr_matrix = corr_analysis['correlation_matrix']
            if not isinstance(corr_matrix, pd.DataFrame) or corr_matrix.empty:
                logger.warning("Пропуск листа 'Корреляции': корреляционная матрица пуста или некорректна")
                return

            # Запись в Excel
            corr_matrix.to_excel(writer, sheet_name='Корреляции', index=True)
            logger.info("Лист 'Корреляции' добавлен в Excel")

        except Exception as e:
            logger.error(f"Критическая ошибка создания листа 'Корреляции': {e}")
            error_df = pd.DataFrame({'Ошибка': [f'Не удалось создать корреляционную матрицу: {str(e)}']})
            error_df.to_excel(writer, sheet_name='Корреляции', index=False)

    def _add_ml_metrics_sheet(self, writer, analysis_results):
        """Добавляет лист с метриками ML-модели с обработкой ошибок"""
        try:
            # Проверка входных данных
            if not analysis_results or not isinstance(analysis_results, dict):
                logger.warning("Пропуск листа 'ML_метрики': analysis_results отсутствует или некорректен")
                return

            if 'ml_model' not in analysis_results:
                logger.warning("Пропуск листа 'ML_метрики': отсутствует раздел 'ml_model'")
                return

            ml_data = analysis_results['ml_model']
            if 'metrics' not in ml_data:
                logger.warning("Пропуск листа 'ML_метрики': отсутствуют метрики модели")
                return

            metrics_data = ml_data['metrics']
            if not isinstance(metrics_data, dict) or not metrics_data:
                logger.warning("Пропуск листа 'ML_метрики': некорректный формат метрик")
                return

            # Создание DataFrame и запись
            metrics_df = pd.DataFrame([metrics_data])
            metrics_df.to_excel(writer, sheet_name='ML_метрики', index=False)
            logger.info("Лист 'ML_метрики' добавлен в Excel")

        except Exception as e:
            logger.error(f"Критическая ошибка создания листа 'ML_метрики': {e}")
            error_df = pd.DataFrame({'Ошибка': [f'Не удалось создать метрики ML: {str(e)}']})
            error_df.to_excel(writer, sheet_name='ML_метрики', index=False)

    

    def _add_outliers_sheet(self, writer, analysis_results, df):
        if df is None or df.empty:
            logger.warning("DataFrame пуст или None — пропускаем создание листа 'Выбросы'")
            return
        """
        Добавляет лист с информацией о выбросах в Excel-файл.

        Args:
            writer: объект pd.ExcelWriter для записи в Excel
            analysis_results: словарь с результатами анализа
            df: исходный DataFrame для расчёта процентов
        """
        try:
            # Проверка на None и тип DataFrame
            if df is None:
                logger.error("DataFrame равен None — невозможно рассчитать проценты выбросов")
                return

            if not isinstance(df, pd.DataFrame):
                logger.error(f"Ожидался DataFrame, но получен {type(df)}")
                return

            # Проверяем наличие 'statistics' и его тип
            if 'statistics' not in analysis_results:
                logger.warning("Раздел 'statistics' отсутствует в результатах анализа")
                return

            statistics_data = analysis_results['statistics']

            # Проверяем, что statistics_data — словарь (итерируемый)
            if not isinstance(statistics_data, dict):
                logger.warning(f"Раздел 'statistics' имеет неверный формат: {type(statistics_data)}")
                return

            # Теперь безопасно проверяем наличие 'outliers'
            if 'outliers' not in statistics_data:
                logger.warning("Данные о выбросах ('outliers') отсутствуют в разделе 'statistics'")
                return

            outliers_data = statistics_data['outliers']

            # Проверяем тип outliers_data
            if not isinstance(outliers_data, dict):
                logger.warning("Данные о выбросах имеют неверный формат (не словарь)")
                return

            # Безопасный расчёт общего количества записей
            total_count = len(df) if not df.empty else 0

            # Дальнейшая обработка outliers_data...
            outliers_summary = {}

            for method, outliers in outliers_data.items():
                if not isinstance(outliers, dict):
                    logger.warning(f"Данные выбросов для метода '{method}' имеют неверный формат")
                    continue

                for col, outlier_info in outliers.items():
                    if not isinstance(outlier_info, dict):
                        logger.warning(f"Информация о выбросах для колонки '{col}' имеет неверный формат")
                        continue


                    indices = outlier_info.get('indices', [])
                    count = len(indices)
                    # Безопасный расчёт процента с учётом пустого DataFrame
                    percentage = (count / total_count * 100) if total_count > 0 else 0

                    outliers_summary[f"{col}_{method}_count"] = count
                    outliers_summary[f"{col}_{method}_percentage"] = round(percentage, 2)


            if outliers_summary:
                outliers_df = pd.DataFrame([outliers_summary])
                outliers_df.to_excel(writer, sheet_name='Выбросы', index=False)
                logger.info("Лист 'Выбросы' добавлен в Excel")
            else:
                logger.warning("Не удалось собрать данные для листа 'Выбросы'")

        except Exception as e:
            logger.error(f"Критическая ошибка создания листа 'Выбросы': {e}")
            raise

    def _add_time_series_sheet(self, writer, analysis_results):
        """Добавляет лист анализа временного ряда с обработкой ошибок"""
        try:
            # Проверка входных данных
            if not analysis_results or not isinstance(analysis_results, dict):
                logger.warning("Пропуск листа 'Временные ряды': analysis_results отсутствует или некорректен")
                return

            if 'trend_analysis' not in analysis_results:
                error_df = pd.DataFrame({'Info': ['Анализ временных рядов недоступен: отсутствует trend_analysis']})
                error_df.to_excel(writer, sheet_name='Временные ряды', index=False)
                return

            trend_stats = analysis_results['trend_analysis']
            if trend_stats is None:
                trend_stats = {}

            def safe_get(data, key, default='Нет данных'):
                """Безопасное получение значения из словаря"""
                if data is None or not isinstance(data, dict):
                    return default
                return data.get(key, default)

            # Сбор данных для таблицы
            trend_data = {
                'Наличие тренда': safe_get(trend_stats, 'has_trend', 'Неизвестно'),
                'Тип тренда': safe_get(trend_stats, 'trend_type', 'Не определён'),
                'Сила тренда (R²)': safe_get(trend_stats, 'r_squared'),
                'P-значение': safe_get(trend_stats, 'p_value'),
                'Наклон': safe_get(trend_stats, 'slope'),
                'Направление тренда': safe_get(trend_stats, 'direction', 'Не определено'),
                'Период сезонности': safe_get(trend_stats, 'seasonal_period', 'Не выявлен')
            }

            # Преобразуем в DataFrame и записываем в Excel
            trend_df = pd.DataFrame([trend_data])

            try:
                trend_df.to_excel(writer, sheet_name='Временные ряды', index=False)
                logger.info("Лист 'Временные ряды' добавлен в Excel")
            except Exception as e:
                logger.error(f"Ошибка записи данных в лист 'Временные ряды': {e}")
                # Создаём лист-заглушку с ошибкой
                worksheet = writer.add_worksheet('Временные ряды')
                worksheet.write('A1', f'Ошибка записи данных: {str(e)}')
                worksheet.write('A2', 'Данные о тренде недоступны')

        except Exception as e:
            logger.critical(f"Критическая ошибка в методе _add_time_series_sheet: {e}")
            # Создаём лист-заглушку на случай критической ошибки
            try:
                worksheet = writer.add_worksheet('Временные ряды')
                worksheet.write('A1', f'Критическая ошибка создания листа: {str(e)}')
                worksheet.write('A2', f'Время ошибки: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            except Exception as sheet_error:
                logger.error(f"Не удалось создать лист-заглушку: {sheet_error}")

    def _add_summary_sheet(self, writer, data, analysis_results):
        """Создаёт сводный лист с основными выводами анализа"""
        try:
            # Проверка входных данных
            if data is None or not isinstance(data, pd.DataFrame):
                logger.warning("Пропуск листа 'Сводка': данные отсутствуют или некорректны")
                return

            if analysis_results is None or not isinstance(analysis_results, dict):
                logger.warning("Пропуск листа 'Сводка': analysis_results отсутствуют или некорректны")
                return

            summary_data = {
                'Общее количество записей': len(data),
                'Количество столбцов': len(data.columns),
                'Период данных': f"{data.index.min()} — {data.index.max()}" if hasattr(data.index, 'min') else 'Не указан',
                'Дата генерации отчёта': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # Добавляем ключевые метрики ML с проверкой
            if 'ml_model' in analysis_results and 'metrics' in analysis_results['ml_model']:
                metrics = analysis_results['ml_model']['metrics']
                if isinstance(metrics, dict):
                    summary_data.update({
                        'Точность модели': f"{metrics.get('accuracy', 'N/A'):.4f}" if 'accuracy' in metrics else 'N/A',
                        'F1-мера': f"{metrics.get('f1_score', 'N/A'):.4f}" if 'f1_score' in metrics else 'N/A',
                'MSE': f"{metrics.get('mse', 'N/A'):.6f}" if 'mse' in metrics else 'N/A'
            })

            # Добавляем информацию о качестве данных
            total_cells = data.size
            missing_cells = data.isnull().sum().sum()
            missing_percentage = (missing_cells / total_cells * 100) if total_cells > 0 else 0
            summary_data['Процент пропусков'] = f"{missing_percentage:.2f}%"

            # Добавляем статистику по выбросам, если доступна
            if ('statistics' in analysis_results
                and 'outliers' in analysis_results['statistics']):
                outliers_data = analysis_results['statistics']['outliers']
                total_outliers = 0
                for method_outliers in outliers_data.values():
                    for col_outliers in method_outliers.values():
                        if isinstance(col_outliers, dict) and 'indices' in col_outliers:
                            total_outliers += len(col_outliers['indices'])
                            summary_data['Общее количество выбросов'] = total_outliers

            # Создаём и записываем DataFrame
            summary_df = pd.DataFrame([summary_data])
            summary_df.to_excel(writer, sheet_name='Сводка', index=False)
            logger.info("Лист 'Сводка' добавлен в Excel")

        except Exception as e:
            logger.error(f"Критическая ошибка создания листа 'Сводка': {e}")
            # Создаём лист с сообщением об ошибке
            error_df = pd.DataFrame({'Ошибка': [f'Не удалось создать сводку: {str(e)}']})
            error_df.to_excel(writer, sheet_name='Сводка', index=False)


# Email sender с флагом allow_send (для тестирования можно отключать реальную отправку)
class EmailSender:
    def __init__(self, config_manager, log_callback: Optional[Callable[[str, str], None]] = None, max_workers: int = 2, allow_send: bool = True):
        self.config_manager = config_manager
        self.log_callback = log_callback
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.allow_send = bool(allow_send)

        smtp_cfg = self.config_manager.get('smtp', {}) if hasattr(self.config_manager, 'get') else {}
        self.smtp_cfg = smtp_cfg
        self.is_configured = bool(self.smtp_cfg.get('host') and self.smtp_cfg.get('user') and self.smtp_cfg.get('password'))

        if not self.is_configured:
            self._log("SMTP настройки неполные. Отправка email будет недоступна.", "warning")
        if not self.allow_send:
            self._log("EmailSender: фактическая отправка отключена (allow_send=False).", "warning")

    def _log(self, msg: str, level: str = "info"):
        if self.log_callback:
            self.log_callback(msg, level.upper())
        logger.log(getattr(logging, level.upper(), logging.INFO), msg)

    def send(self, subject: str, html_body: str, attachments: Optional[List[str]] = None, recipients: Optional[List[str]] = None) -> bool:
        if not self.allow_send:
            self._log("Отправка отключена (allow_send=False). Симуляция успешной отправки.", "info")
            return True

        if not self.is_configured:
            self._log("Попытка отправить email, но SMTP не настроен.", "error")
            return False
        if not recipients:
            self._log("Список получателей пуст. Email не будет отправлен.", "error")
            return False

        host = self.smtp_cfg.get('host')
        port = int(self.smtp_cfg.get('port', 587))
        sender_email = self.smtp_cfg.get('user')
        password = self.smtp_cfg.get('password')
        use_tls = self.smtp_cfg.get('use_tls', True)

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipients)
        msg.set_content("Пожалуйста, используйте клиент с поддержкой HTML для просмотра этого сообщения.")
        msg.add_alternative(html_body or "", subtype='html')

        attachments_names = []
        for path_str in (attachments or []):
            path = Path(path_str)
            if not path.exists():
                self._log(f"Файл вложения не найден: {path}. Пропуск.", "warning")
                continue
            try:
                with open(path, 'rb') as f:
                    file_data = f.read()
                ctype, _ = mimetypes.guess_type(str(path))
                if ctype is None:
                    ctype = 'application/octet-stream'
                maintype, subtype = ctype.split('/', 1)
                msg.add_attachment(file_data, maintype=maintype, subtype=subtype, filename=path.name)
                attachments_names.append(path.name)
            except Exception as e:
                self._log(f"Не удалось прикрепить {path.name}: {e}", "error")

        if attachments_names:
            self._log(f"Будут прикреплены файлы: {', '.join(attachments_names)}", "info")

        try:
            if not host or not port or not sender_email or not password:
                self._log("Недостаточно данных для SMTP. Email не будет отправлен.", "error")
                return False
            server = smtplib.SMTP(host, port, timeout=30)
            if use_tls:
                server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
            server.quit()
            self._log(f"Email успешно отправлен на: {', '.join(recipients)}", "info")
            return True
        except smtplib.SMTPAuthenticationError as e:
            self._log(f"Ошибка аутентификации SMTP: {e}", "error")
            return False
        except Exception as e:
            self._log(f"Ошибка при отправке email: {e}", "error")
            return False

    def send_async(self, subject: str, html_body: str, attachments: Optional[List[str]] = None, recipients: Optional[List[str]] = None, callback: Optional[Callable] = None) -> concurrent.futures.Future:
        if not self.allow_send:
            self._log("Асинхронная отправка отключена (allow_send=False). Возвращаем Future с True.", "info")
            fut = self._executor.submit(lambda: True)
            if callback:
                fut.add_done_callback(lambda f: callback(True))
            return fut

        if not self.is_configured:
            self._log("Попытка асинхронной отправки, но SMTP не настроен.", "error")
            fut = self._executor.submit(lambda: False)
            if callback:
                fut.add_done_callback(lambda f: callback(False))
            return fut

        future = self._executor.submit(self.send, subject, html_body, attachments, recipients)
        if callback:
            def wrapped(f: concurrent.futures.Future):
                try:
                    res = f.result()
                    callback(res)
                except Exception as e:
                    self._log(f"Ошибка в колбэке асинхронной отправки: {e}", "error")
                    callback(False)
            future.add_done_callback(wrapped)
        return future

    def shutdown(self):
        if hasattr(self, '_executor') and self._executor:
            self._executor.shutdown()


# ReportManager
class ReportManager:
    def __init__(self, config_manager, log_callback: Optional[Callable[[str, str], None]] = None, max_workers: int = 2, email_allow_send: bool = True):
        self.config_manager = config_manager
        self.log_callback = log_callback
        self._report_executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._last_generated: Dict[str, str] = {}
        self._email_sender = None

        smtp_cfg = self.config_manager.get('smtp', {}) if hasattr(self.config_manager, 'get') else {}
        if smtp_cfg.get('host') and smtp_cfg.get('user') and smtp_cfg.get('password'):
            # передаём allow_send флаг в EmailSender
            self._email_sender = EmailSender(self.config_manager, self.log_callback, max_workers=max_workers, allow_send=email_allow_send)
        else:
            self._log("SMTP настройки неполные. Email отправка недоступна.", "warning")

    def _log(self, msg: str, level: str = "info"):
        if self.log_callback:
            self.log_callback(msg, level.upper())
        logger.log(getattr(logging, level.upper(), logging.INFO), msg)

    def _generate_html_summary(self, analysis_results: Dict[str, Any]) -> str:
        stats = analysis_results.get('statistics', {}) if isinstance(analysis_results, dict) else {}
        rows = ""
        if isinstance(stats, dict):
            count = 0
            for col_name, col_stats in list(stats.items()):
                if count >= 5:
                    break
                if isinstance(col_stats, dict):
                    mean_val = col_stats.get('mean', 0) or 0
                    min_val = col_stats.get('min', 0) or 0
                    max_val = col_stats.get('max', 0) or 0
                    rows += f"<tr><td>{col_name}</td><td>{mean_val:.2f}</td><td>{min_val:.2f}</td><td>{max_val:.2f}</td></tr>"
                    count += 1
        if not rows:
            rows = "<tr><td colspan='4'>Нет данных для отображения сводки.</td></tr>"
        return f"""
        <html><body>
        <h2>Результаты анализа данных</h2>
        <table border="1" style="border-collapse: collapse;"><thead>
        <tr><th>Колонка</th><th>Среднее</th><th>Мин.</th><th>Макс.</th></tr></thead><tbody>{rows}</tbody></table>
        <p>Полные отчеты прикреплены.</p>
        </body></html>
        """

    def generate_reports(
        self,
        df: Optional[pd.DataFrame],
        analysis_results: Dict[str, Any],
        output_dir: str = "reports",
        output_format: str = "pdf",
        send_email: bool = False,
        email_recipients: Optional[List[str]] = None,
        report_name_prefix: str = "Report"
    ) -> Dict[str, str]:
        if output_format not in ('pdf', 'excel', 'both'):
            self._log(f"Неподдерживаемый формат: {output_format}. Используем pdf.", "warning")
            output_format = 'pdf'

        Path(output_dir).mkdir(parents=True, exist_ok=True)

        generated: Dict[str, str] = {}
        native_results = _ensure_native(analysis_results or {})
        images_from_results = native_results.get('visualizations', []) if isinstance(native_results, dict) else []
        valid_images = [str(p) for p in images_from_results if isinstance(p, str) and Path(p).is_file()] if isinstance(images_from_results, list) else []

        # PDF
        pdf_path = None
        if output_format in ('pdf', 'both'):
            pdf_output_path = str(Path(output_dir) / f"{report_name_prefix}.pdf")
            pdf_gen = PDFReportGenerator(pdf_output_path, self.log_callback)
            try:
                pdf_path, _ = pdf_gen.create_pdf(native_results, df=df, images=valid_images)
                generated['pdf'] = pdf_path
                self._log(f"PDF сгенерирован: {pdf_path}", "info")
            except Exception as e:
                self._log(f"Ошибка генерации PDF: {e}", "error")

        # Excel (включаем всегда при request)
        if output_format in ('excel', 'both'):
            excel_output_path = str(Path(output_dir) / f"{report_name_prefix}.xlsx")
            excel_gen = ExcelReportGenerator(excel_output_path, self.log_callback)
            try:
                # Используем df как data, если он есть, иначе — пустой DataFrame
                data_for_excel = df if df is not None and isinstance(df, pd.DataFrame) else pd.DataFrame()

                excel_path = excel_gen.export_to_excel(
                    df=df,  # передаём исходный DataFrame
                    data=data_for_excel,  # используем подготовленный DataFrame для листа «Исходные данные»
                    analysis_results=analysis_results,
                    output_path=excel_output_path
                )
                generated['excel'] = excel_path
                self._log(f"Excel сгенерирован: {excel_path}", "info")
            except Exception as e:
                self._log(f"Ошибка генерации Excel: {e}", "error")

        # Сохраняем last generated
        try:
            self._last_generated = dict(generated)
        except Exception:
            self._last_generated = {}

        # Отправка email (если указано)
        if send_email and self._email_sender:
            recipients = email_recipients or []
            if recipients:
                subject = f"Отчёт: {report_name_prefix}"
                body = self._generate_html_summary(native_results)
                attachments = [generated[k] for k in generated if k in ('pdf', 'excel') and Path(generated[k]).is_file()]
                try:
                    self._email_sender.send_async(subject, body, attachments=attachments, recipients=recipients, callback=self._email_send_callback)
                    self._log(f"Запрошена отправка email на: {', '.join(recipients)}", "info")
                except Exception as e:
                    self._log(f"Ошибка при постановке задачи отправки email: {e}", "error")
            else:
                self._log("send_email=True, но нет получателей. Отправка пропущена.", "warning")

        return generated

    def get_last_generated(self) -> Dict[str, str]:
        return dict(self._last_generated or {})

    def send_reports(self, report_paths: Dict[str, str], recipients: List[str], analysis_results: Optional[Dict[str, Any]] = None, subject: Optional[str] = None, async_send: bool = True) -> bool:
        if not report_paths:
            self._log("send_reports вызван без report_paths.", "warning")
            return False
        if not recipients:
            self._log("send_reports вызван без recipients.", "warning")
            return False
        if not self._email_sender:
            self._log("EmailSender не настроен, отправка невозможна.", "error")
            return False

        attachments = [p for p in report_paths.values() if isinstance(p, str) and Path(p).is_file()]
        if not attachments:
            self._log("Нет доступных файлов для отправки.", "warning")
            return False

        subj = subject or f"Автоматический отчет: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        body = self._generate_html_summary(analysis_results or {})

        if async_send:
            self._email_sender.send_async(subj, body, attachments=attachments, recipients=recipients, callback=self._email_send_callback)
            self._log("Асинхронная отправка запущена.", "info")
            return True
        else:
            return self._email_sender.send(subj, body, attachments=attachments, recipients=recipients)

    def send_email(self, file_path: str, recipients: List[str], subject: Optional[str] = None, async_send: bool = True) -> bool:
        if not file_path or not Path(file_path).is_file():
            self._log(f"send_email: недействительный путь {file_path}", "error")
            return False
        return self.send_reports({'file': file_path}, recipients, analysis_results=None, subject=subject, async_send=async_send)

    def _email_send_callback(self, success: bool):
        if success:
            self._log("Email успешно отправлен (callback).", "info")
        else:
            self._log("Ошибка отправки email (callback).", "error")

    def generate_reports_async(self, df: Optional[pd.DataFrame], analysis_results: Dict[str, Any], output_dir: str = "reports", output_format: str = "pdf", send_email: bool = False, email_recipients: Optional[List[str]] = None, report_name_prefix: Optional[str] = None, callback: Optional[Callable[[bool], None]] = None) -> concurrent.futures.Future:
        final_prefix = report_name_prefix or "Report"
        future = self._report_executor.submit(self.generate_reports, df, analysis_results, output_dir, output_format, send_email, email_recipients, final_prefix)
        if callback:
            def wrapped(f: concurrent.futures.Future):
                try:
                    generated = f.result()
                    callback(bool(generated))
                except Exception as e:
                    self._log(f"Ошибка в callback generate_reports_async: {e}", "error")
                    callback(False)
            future.add_done_callback(wrapped)
        self._log(f"Запущена асинхронная генерация отчёта с префиксом {final_prefix}.", "info")
        return future

    def shutdown(self):
        if self._report_executor:
            self._report_executor.shutdown()
        if self._email_sender:
            self._email_sender.shutdown()
        self._log("ReportManager остановлен.", "info")


# NullReportManager для тестов/фолбэка
class NullReportManager:
    def __init__(self, log_callback: Optional[Callable[[str, str], None]] = None):
        self.log_callback = log_callback
        self._report_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._last_generated: Dict[str, str] = {}

    def _log(self, msg: str, level: str = "info"):
        if self.log_callback:
            self.log_callback(msg, level.upper())
        logger.log(getattr(logging, level.upper(), logging.INFO), msg)

    def generate_reports(self, df: Optional[pd.DataFrame], analysis_results: Dict[str, Any], output_dir: str = "reports", output_format: str = "pdf", send_email: bool = False, email_recipients: Optional[List[str]] = None, report_name_prefix: str = "Report") -> Dict[str, str]:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        generated: Dict[str, str] = {}

        if output_format in ('pdf', 'both'):
            pdf_path = str(Path(output_dir) / f"{report_name_prefix}.pdf")
            with open(pdf_path, 'w') as f:
                f.write("Dummy PDF\n")
            generated['pdf'] = pdf_path
            self._log(f"Null: сгенерирован {pdf_path}", "info")

        if output_format in ('excel', 'both'):
            excel_path = str(Path(output_dir) / f"{report_name_prefix}.xlsx")
            try:
                workbook = openpyxl.Workbook()
                sheet = workbook.active
                if not sheet:
                    self._log("Null: не удалось создать активный лист в excel.", "error")
                    return generated
                sheet.title = "Dummy Data"
                sheet['A1'] = "Dummy"
                workbook.save(excel_path)
                generated['excel'] = excel_path
                self._log(f"Null: сгенерирован {excel_path}", "info")
            except Exception as e:
                self._log(f"Null: ошибка при создании excel {e}", "error")

        self._last_generated = dict(generated)
        return generated

    def get_last_generated(self) -> Dict[str, str]:
        return dict(self._last_generated or {})

    def send_reports(self, report_paths: Dict[str, str], recipients: List[str], analysis_results: Optional[Dict[str, Any]] = None, subject: Optional[str] = None, async_send: bool = True) -> bool:
        if not report_paths or not recipients:
            self._log("Null: send_reports вызван без параметров.", "warning")
            return False
        self._log(f"Null: имитация отправки {len(report_paths)} файла(ов) на {', '.join(recipients)}", "info")
        return True

    def send_email(self, file_path: str, recipients: List[str], subject: Optional[str] = None, async_send: bool = True) -> bool:
        if not file_path or not recipients:
            self._log("Null: send_email вызван с некорректными параметрами.", "warning")
            return False
        self._log(f"Null: имитация отправки файла {file_path} на {', '.join(recipients)}", "info")
        return True

    def generate_reports_async(self, df: Optional[pd.DataFrame], analysis_results: Dict[str, Any], output_dir: str = "reports", output_format: str = "pdf", send_email: bool = False, email_recipients: Optional[List[str]] = None, report_name_prefix: Optional[str] = None, callback: Optional[Callable[[bool], None]] = None) -> concurrent.futures.Future:
        final_prefix = report_name_prefix or "Report"
        future = self._report_executor.submit(self.generate_reports, df, analysis_results, output_dir, output_format, False, None, final_prefix)
        if callback:
            def wrapped(f: concurrent.futures.Future):
                try:
                    gen = f.result()
                    callback(bool(gen))
                except Exception as e:
                    self._log(f"Null: ошибка в callback async: {e}", "error")
                    callback(False)
            future.add_done_callback(wrapped)
        return future

    def shutdown(self):
        if self._report_executor:
            self._report_executor.shutdown()
            self._log("NullReportManager остановлен.", "info")
