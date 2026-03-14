
# language: python
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

# Устанавливаем неблокирующий backend для matplotlib — важно делать это ДО импорта pyplot
import matplotlib
matplotlib.use("Agg")  # безопасный backend для фоновой генерации графиков/PDF

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
            logger.exception("PDF creation exception")
            raise


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
        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Используем openpyxl как engine (установлен в окружении)
            with pd.ExcelWriter(str(out_path), engine='openpyxl') as writer:
                # 1. Исходные данные
                if data is not None and isinstance(data, pd.DataFrame) and not data.empty:
                    try:
                        data.to_excel(writer, sheet_name='Исходные_данные', index=False)
                        logger.info("Лист 'Исходные_данные' добавлен в Excel")
                    except Exception:
                        logger.exception("Ошибка записи листа 'Исходные_данные'")
                        self._create_error_sheet(writer, 'Исходные_данные', 'Ошибка записи исходных данных')

                else:
                    logger.warning(
                        "Пропуск листа 'Исходные_данные': "
                        f"data={type(data) if data is not None else 'None'}, "
                        f"пустой={data.empty if isinstance(data, pd.DataFrame) else 'N/A'}"
                    )

                analysis_available = analysis_results and isinstance(analysis_results, dict)

                # 2. Статистика
                try:
                    if analysis_available:
                        self._add_statistics_sheet(writer, analysis_results)
                    else:
                        self._create_error_sheet(writer, 'Статистика', 'Отсутствуют данные analysis_results')
                except Exception:
                    logger.exception("Ошибка при добавлении листа 'Статистика'")
                    self._create_error_sheet(writer, 'Статистика', 'Критическая ошибка при создании статистики')

                # 3. Корреляции
                try:
                    if analysis_available:
                        self._add_correlations_sheet(writer, analysis_results)
                    else:
                        self._create_error_sheet(writer, 'Корреляции', 'Отсутствуют данные analysis_results')
                except Exception:
                    logger.exception("Ошибка при добавлении листа 'Корреляции'")
                    self._create_error_sheet(writer, 'Корреляции', 'Критическая ошибка при создании корреляций')

                # 4. ML метрики
                try:
                    if analysis_available:
                        self._add_ml_metrics_sheet(writer, analysis_results)
                    else:
                        self._create_error_sheet(writer, 'ML_метрики', 'Отсутствуют данные analysis_results')
                except Exception:
                    logger.exception("Ошибка при добавлении листа 'ML_метрики'")
                    self._create_error_sheet(writer, 'ML_метрики', 'Критическая ошибка при создании ML метрик')

                # 5. Выбросы
                try:
                    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                        logger.warning("Пропуск листа 'Выбросы' из-за отсутствия данных")
                    else:
                        self._add_outliers_sheet(writer, analysis_results, df)
                except Exception:
                    logger.exception("Ошибка при добавлении листа 'Выбросы'")
                    self._create_error_sheet(writer, 'Выбросы', 'Критическая ошибка при создании листа Выбросы')

                # 6. Временные ряды
                try:
                    self._add_time_series_sheet(writer, analysis_results)
                except Exception:
                    logger.exception("Ошибка при добавлении листа 'Временные ряды'")
                    self._create_error_sheet(writer, 'Временные ряды', 'Критическая ошибка при создании листа Временные ряды')

                # 7. Сводка
                try:
                    if (data is not None and isinstance(data, pd.DataFrame)) and analysis_available:
                        self._add_summary_sheet(writer, data, analysis_results)
                    else:
                        self._create_error_sheet(writer, 'Сводка', 'Недостаточно данных для сводки')
                except Exception:
                    logger.exception("Ошибка при добавлении листа 'Сводка'")
                    self._create_error_sheet(writer, 'Сводка', 'Критическая ошибка при создании сводки')

            logger.info(f"Excel-отчёт успешно создан: {out_path}")
            return str(out_path)
        except Exception as e:
            logger.exception("Критическая ошибка при создании Excel-отчёта")
            raise

    def _add_statistics_sheet(self, writer, analysis_results):
        try:
            if not analysis_results or not isinstance(analysis_results, dict):
                logger.warning("Пропуск листа 'Статистика': analysis_results отсутствует или некорректен")
                return

            stats_section = analysis_results.get('statistics', {})
            if not isinstance(stats_section, dict):
                logger.warning("Пропуск листа 'Статистика': statistics некорректен")
                return

            stats_data = stats_section.get('extended', {})
            if not stats_data or not isinstance(stats_data, dict):
                logger.warning("Пропуск листа 'Статистика': extended пуст или некорректен")
                return

            if not any(stats_data.values()):
                logger.warning("Пропуск листа 'Статистика': нет данных для отображения")
                return

            try:
                stats_df = pd.DataFrame(stats_data).T
                if stats_df.empty:
                    logger.warning("Пустой DataFrame для статистики — пропуск")
                    return
                stats_df.to_excel(writer, sheet_name='Статистика', index=True)
                logger.info(f"Лист 'Статистика' добавлен в Excel. Столбцов: {len(stats_df.columns)}")
            except Exception:
                logger.exception("Ошибка преобразования/записи статистики")
                self._create_error_sheet(writer, 'Статистика', 'Ошибка формирования таблицы статистики')

        except Exception:
            logger.exception("Критическая ошибка в _add_statistics_sheet")
            self._create_error_sheet(writer, 'Статистика', 'Критическая ошибка метода')

    def _create_error_sheet(self, writer, sheet_name: str, error_message: str):
        try:
            error_df = pd.DataFrame({
                'Ошибка при создании листа': [error_message],
                'Время ошибки': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            error_df.to_excel(writer, sheet_name=sheet_name, index=False)
            logger.warning(f"Создан лист-заглушка '{sheet_name}' с сообщением об ошибке")
        except Exception:
            logger.exception(f"Не удалось создать лист-заглушку '{sheet_name}'")

    def _add_correlations_sheet(self, writer, analysis_results):
        try:
            stats_data = analysis_results.get('statistics', {}) if isinstance(analysis_results, dict) else {}
            corr_analysis = stats_data.get('correlation_analysis', {}) if isinstance(stats_data, dict) else {}
            corr_matrix = corr_analysis.get('correlation_matrix', None)

            if not isinstance(corr_matrix, pd.DataFrame) or corr_matrix.empty:
                logger.warning("Пропуск листа 'Корреляции': корреляционная матрица пуста или некорректна")
                return

            corr_matrix.to_excel(writer, sheet_name='Корреляции', index=True)
            logger.info("Лист 'Корреляции' добавлен в Excel")

        except Exception:
            logger.exception("Критическая ошибка создания листа 'Корреляции'")
            self._create_error_sheet(writer, 'Корреляции', 'Ошибка при создании корреляций')

    def _add_ml_metrics_sheet(self, writer, analysis_results):
        try:
            ml_data = analysis_results.get('ml_model', {}) if isinstance(analysis_results, dict) else {}
            metrics_data = ml_data.get('metrics', None) if isinstance(ml_data, dict) else None

            if not isinstance(metrics_data, dict) or not metrics_data:
                logger.warning("Пропуск листа 'ML_метрики': метрики пусты или некорректны")
                return

            metrics_df = pd.DataFrame([metrics_data])
            metrics_df.to_excel(writer, sheet_name='ML_метрики', index=False)
            logger.info("Лист 'ML_метрики' добавлен в Excel")

        except Exception:
            logger.exception("Критическая ошибка создания листа 'ML_метрики'")
            self._create_error_sheet(writer, 'ML_метрики', 'Ошибка при создании ML метрик')

    def _add_outliers_sheet(self, writer, analysis_results, df):
        try:
            if df is None or df.empty:
                logger.warning("Пропуск листа 'Выбросы': df пустой или None")
                return

            statistics_data = analysis_results.get('statistics', {}) if isinstance(analysis_results, dict) else {}
            outliers_data = statistics_data.get('outliers', {}) if isinstance(statistics_data, dict) else {}

            if not isinstance(outliers_data, dict) or not outliers_data:
                logger.warning("Пропуск листа 'Выбросы': данные о выбросах отсутствуют")
                return

            total_count = len(df) if not df.empty else 0
            outliers_summary = {}

            for method, outliers in outliers_data.items():
                if not isinstance(outliers, dict):
                    logger.warning(f"Формат выбросов для метода {method} некорректен")
                    continue
                for col, outlier_info in outliers.items():
                    if not isinstance(outlier_info, dict):
                        logger.warning(f"Формат информации о выбросах для {col} некорректен")
                        continue
                    indices = outlier_info.get('indices', [])
                    count = len(indices)
                    percentage = (count / total_count * 100) if total_count > 0 else 0
                    outliers_summary[f"{col}_{method}_count"] = count
                    outliers_summary[f"{col}_{method}_percentage"] = round(percentage, 2)

            if outliers_summary:
                outliers_df = pd.DataFrame([outliers_summary])
                outliers_df.to_excel(writer, sheet_name='Выбросы', index=False)
                logger.info("Лист 'Выбросы' добавлен в Excel")
            else:
                logger.warning("Не удалось собрать данные для листа 'Выбросы'")

        except Exception:
            logger.exception("Критическая ошибка создания листа 'Выбросы'")
            self._create_error_sheet(writer, 'Выбросы', 'Критическая ошибка при создании листа Выбросы')

    def _add_time_series_sheet(self, writer, analysis_results):
        try:
            if not analysis_results or not isinstance(analysis_results, dict):
                logger.warning("Пропуск листа 'Временные ряды': analysis_results отсутствует или некорректен")
                return

            if 'trend_analysis' not in analysis_results:
                error_df = pd.DataFrame({'Info': ['Анализ временных рядов недоступен: отсутствует trend_analysis']})
                error_df.to_excel(writer, sheet_name='Временные ряды', index=False)
                return

            trend_stats = analysis_results.get('trend_analysis') or {}

            def safe_get(data, key, default='Нет данных'):
                if data is None or not isinstance(data, dict):
                    return default
                return data.get(key, default)

            trend_data = {
                'Наличие тренда': safe_get(trend_stats, 'has_trend', 'Неизвестно'),
                'Тип тренда': safe_get(trend_stats, 'trend_type', 'Не определён'),
                'Сила тренда (R²)': safe_get(trend_stats, 'r_squared'),
                'P-значение': safe_get(trend_stats, 'p_value'),
                'Наклон': safe_get(trend_stats, 'slope'),
                'Направление тренда': safe_get(trend_stats, 'direction', 'Не определено'),
                'Период сезонности': safe_get(trend_stats, 'seasonal_period', 'Не выявлен')
            }

            trend_df = pd.DataFrame([trend_data])
            try:
                trend_df.to_excel(writer, sheet_name='Временные ряды', index=False)
                logger.info("Лист 'Временные ряды' добавлен в Excel")
            except Exception:
                logger.exception("Ошибка записи листа 'Временные ряды'")
                self._create_error_sheet(writer, 'Временные ряды', 'Ошибка записи данных о тренде')

        except Exception:
            logger.exception("Критическая ошибка в методе _add_time_series_sheet")
            self._create_error_sheet(writer, 'Временные ряды', 'Критическая ошибка метода')

    def _add_summary_sheet(self, writer, data, analysis_results):
        try:
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

            if 'ml_model' in analysis_results and 'metrics' in analysis_results['ml_model']:
                metrics = analysis_results['ml_model']['metrics']
                if isinstance(metrics, dict):
                    summary_data.update({
                        'Точность модели': f"{metrics.get('accuracy', 'N/A'):.4f}" if 'accuracy' in metrics else 'N/A',
                        'F1-мера': f"{metrics.get('f1_score', 'N/A'):.4f}" if 'f1_score' in metrics else 'N/A',
                        'MSE': f"{metrics.get('mse', 'N/A'):.6f}" if 'mse' in metrics else 'N/A'
                    })

            total_cells = data.size
            missing_cells = data.isnull().sum().sum()
            missing_percentage = (missing_cells / total_cells * 100) if total_cells > 0 else 0
            summary_data['Процент пропусков'] = f"{missing_percentage:.2f}%"

            if ('statistics' in analysis_results and 'outliers' in analysis_results['statistics']):
                outliers_data = analysis_results['statistics']['outliers']
                total_outliers = 0
                for method_outliers in outliers_data.values():
                    for col_outliers in method_outliers.values():
                        if isinstance(col_outliers, dict) and 'indices' in col_outliers:
                            total_outliers += len(col_outliers['indices'])
                            summary_data['Общее количество выбросов'] = total_outliers

            summary_df = pd.DataFrame([summary_data])
            summary_df.to_excel(writer, sheet_name='Сводка', index=False)
            logger.info("Лист 'Сводка' добавлен в Excel")

        except Exception:
            logger.exception("Критическая ошибка создания листа 'Сводка'")
            error_df = pd.DataFrame({'Ошибка': [f'Не удалось создать сводку']})
            error_df.to_excel(writer, sheet_name='Сводка', index=False)


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

    def send_async(self, subject: str, html_body: str, attachments: Optional[List[str]] = None,
                   recipients: Optional[List[str]] = None) -> concurrent.futures.Future:
        """
        Возвращает concurrent.futures.Future. ВНИМАНИЕ: колбэки, добавляемые через future.add_done_callback(),
        будут вызваны в потокe Executor'а. Если нужно обновлять GUI — используйте root.after(...) из main thread
        внутри callback (или лучше: добавляйте done_callback в main thread).
        """
        if not self.allow_send:
            self._log("Асинхронная отправка отключена (allow_send=False). Возвращаем Future с True.", "info")
            return self._executor.submit(lambda: True)

        if not self.is_configured:
            self._log("Попытка асинхронной отправки, но SMTP не настроен.", "error")
            return self._executor.submit(lambda: False)

        future = self._executor.submit(self.send, subject, html_body, attachments, recipients)
        # Не вызываем callback здесь — возвращаем Future и даём вызывающему подписаться,
        # чтобы он мог обеспечить исполнение callback в main loop (через after).
        return future

    def shutdown(self):
        if hasattr(self, '_executor') and self._executor:
            self._executor.shutdown()


class ReportManager:
    def __init__(self, config_manager, log_callback: Optional[Callable[[str, str], None]] = None, max_workers: int = 2, email_allow_send: bool = True):
        self.config_manager = config_manager
        self.log_callback = log_callback
        self._report_executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._last_generated: Dict[str, str] = {}
        self._email_sender = None

        smtp_cfg = self.config_manager.get('smtp', {}) if hasattr(self.config_manager, 'get') else {}
        if smtp_cfg.get('host') and smtp_cfg.get('user') and smtp_cfg.get('password'):
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

        # создаём структуру папок reports/pdf и reports/excel
        base = Path(output_dir)
        pdf_dir = base / "pdf"
        excel_dir = base / "excel"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        excel_dir.mkdir(parents=True, exist_ok=True)

        generated: Dict[str, str] = {}
        native_results = _ensure_native(analysis_results or {})
        images_from_results = native_results.get('visualizations', []) if isinstance(native_results, dict) else []
        valid_images = [str(p) for p in images_from_results if isinstance(p, str) and Path(p).is_file()] if isinstance(images_from_results, list) else []

        # PDF
        pdf_path = None
        if output_format in ('pdf', 'both'):
            pdf_output_path = str(pdf_dir / f"{report_name_prefix}.pdf")
            pdf_gen = PDFReportGenerator(pdf_output_path, self.log_callback)
            try:
                pdf_path, _ = pdf_gen.create_pdf(native_results, df=df, images=valid_images)
                generated['pdf'] = pdf_path
                self._log(f"PDF сгенерирован: {pdf_path}", "info")
            except Exception:
                logger.exception("Ошибка генерации PDF")
                self._log("Ошибка генерации PDF. Подробности в логах.", "error")

        # Excel
        if output_format in ('excel', 'both'):
            excel_output_path = str(excel_dir / f"{report_name_prefix}.xlsx")
            excel_gen = ExcelReportGenerator(excel_output_path, self.log_callback)
            try:
                data_for_excel = df if df is not None and isinstance(df, pd.DataFrame) else pd.DataFrame()
                excel_path = excel_gen.export_to_excel(
                    df=df,
                    data=data_for_excel,
                    analysis_results=analysis_results,
                    output_path=excel_output_path
                )
                generated['excel'] = excel_path
                self._log(f"Excel сгенерирован: {excel_path}", "info")
            except Exception:
                logger.exception("Ошибка генерации Excel")
                self._log("Ошибка генерации Excel. Подробности в логах.", "error")

        try:
            self._last_generated = dict(generated)
        except Exception:
            self._last_generated = {}

        # Email отправка
        if send_email and self._email_sender:
            recipients = email_recipients or []
            if recipients:
                subject = f"Отчёт: {report_name_prefix}"
                body = self._generate_html_summary(native_results)
                attachments = [generated[k] for k in generated if k in ('pdf', 'excel') and Path(generated[k]).is_file()]
                try:
                    self._email_sender.send_async(subject, body, attachments=attachments, recipients=recipients)
                    self._log(f"Запрошена отправка email на: {', '.join(recipients)}", "info")
                except Exception:
                    logger.exception("Ошибка постановки задачи отправки email")
                    self._log("Ошибка при постановке задачи отправки email. Подробности в логах.", "error")
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
            self._email_sender.send_async(subj, body, attachments=attachments, recipients=recipients)
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
                except Exception:
                    logger.exception("Ошибка в callback generate_reports_async")
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
        base = Path(output_dir)
        pdf_dir = base / "pdf"
        excel_dir = base / "excel"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        excel_dir.mkdir(parents=True, exist_ok=True)

        generated: Dict[str, str] = {}

        if output_format in ('pdf', 'both'):
            pdf_path = str(pdf_dir / f"{report_name_prefix}.pdf")
            with open(pdf_path, 'w', encoding='utf-8') as f:
                f.write("Dummy PDF\n")
            generated['pdf'] = pdf_path
            self._log(f"Null: сгенерирован {pdf_path}", "info")

        if output_format in ('excel', 'both'):
            excel_path = str(excel_dir / f"{report_name_prefix}.xlsx")
            try:
                workbook = openpyxl.Workbook()
                if not workbook:
                    self._log("Null: не удалось создать excel", "error")
                    return {}
                if not workbook.sheetnames:
                    self._log("Null: excel не содержит листов", "warning")
                    return {}
                sheet = workbook.active
                if not sheet:
                    self._log("Null: excel не содержит активного листа", "warning")
                    return {}
                sheet.title = "Dummy Data"
                sheet['A1'] = "Dummy"
                workbook.save(excel_path)
                generated['excel'] = excel_path
                self._log(f"Null: сгенерирован {excel_path}", "info")
            except Exception:
                logger.exception("Null: ошибка при создании excel")

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

    def generate_reports_async(self, df: Optional[pd.DataFrame], analysis_results: Dict[str, Any],
                               output_dir: str = "reports", output_format: str = "pdf",
                               send_email: bool = False, email_recipients: Optional[List[str]] = None,
                               report_name_prefix: Optional[str] = None) -> concurrent.futures.Future:
        """
        Запускает генерацию отчётов в executor и возвращает Future.
        ВНИМАНИЕ: любые колбэки, привязанные через future.add_done_callback(), будут вызваны в worker-потоке.
        GUI-код должен планировать UI-обновления через root.after(...) или подписываться на Future в main thread.
        """
        final_prefix = report_name_prefix or "Report"
        future = self._report_executor.submit(self.generate_reports, df, analysis_results, output_dir, output_format, send_email, email_recipients, final_prefix)
        self._log(f"Запущена асинхронная генерация отчёта с префиксом {final_prefix}.", "info")
        return future

    def shutdown(self):
        if self._report_executor:
            self._report_executor.shutdown()
            self._log("NullReportManager остановлен.", "info")
