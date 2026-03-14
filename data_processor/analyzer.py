
import logging
import math
import os
from typing import Dict, Any, Optional, List, Union, cast, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numbers
from enum import Enum, auto # Для моделирования типа модели

# ML Tools
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression # Не используется в current code, но может быть полезен
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, f1_score, classification_report

# Stats & Time Series
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller

logger = logging.getLogger(__name__)

# --- Вспомогательные функции и классы ---

def _to_native(value: Any) -> Any:
    """Рекурсивно приводит numpy/pandas типы к нативным python типам."""
    if value is None:
        return None

    # Особая обработка для DataFrame/Series
    if isinstance(value, pd.DataFrame):
        return [_to_native(row) for row in value.to_dict(orient='records')]
    # Для Series/Index/ndarray
    if isinstance(value, (pd.Series, pd.Index, np.ndarray)):
        return [_to_native(x) for x in value.tolist()]
    # Для словарей
    if isinstance(value, dict):
        return {str(k) if k is not None else None: _to_native(v) for k, v in value.items()}
    # Для коллекций
    if isinstance(value, (list, tuple, set)):
        return [_to_native(x) for x in value]

    # Обработка специальных значений
    try:
        if pd.isna(value):
            return None
        if isinstance(value, numbers.Real):
            # math.isinf/isnan работают только с float, но проверка numbers.Real это гарантирует
            if math.isinf(float(value)) or math.isnan(float(value)):
                return None
    except (TypeError, ValueError, OverflowError):
        pass

    # Преобразования типов
    if isinstance(value, np.generic):
        return _to_native(value.item())
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        return float(value)

    # Возврат по умолчанию
    return str(value) if not isinstance(value, (str, int, float, bool, type(None))) else value


class ModelType(Enum):
    REGRESSION = auto()
    CLASSIFICATION = auto()

class DataAnalyzer:
    def __init__(self, default_vis_dir: str = "reports/visualizations"):
        self.results: Dict[str, Any] = {}
        self._reset_results()
        self.default_vis_dir = default_vis_dir # Теперь директория для визуализаций настраиваемая
        # Параметры для настройки
        self.outlier_threshold_percentage = 5.0 # Процент выбросов для вывода в insights
        self.high_correlation_threshold = 0.8 # Порог корреляции для вывода в insights
        self.min_ts_days = 14 # Минимальное количество дней для анализа временных рядов
        self.ml_test_size = 0.2 # Размер тестовой выборки для ML
        self.ml_random_state = 42 # Random state для воспроизводимости ML

    def _reset_results(self):
        """Сбрасывает словарь результатов анализа."""
        self.results = {
            'statistics': {},
            'outliers': {},
            'insights': [],
            'time_series': {'success': False},
            'ml_model': {'success': False, 'metrics': {}},
            'correlations': {},
            'visualizations': []
        }

    def analyze(self, df: pd.DataFrame, target_col: Optional[str] = None,
                date_col: Optional[str] = None, model_type: ModelType = ModelType.REGRESSION) -> Dict[str, Any]:
        """
        Основной метод для выполнения полного анализа данных.

        Args:
            df: DataFrame с данными.
            target_col: Название колонки цели (для ML и временных рядов).
            date_col: Название колонки с датой/временем (для временных рядов).
            model_type: Тип модели ('REGRESSION' или 'CLASSIFICATION').

        Returns:
            Словарь с результатами анализа, приведенными к нативным Python типам.
        """
        self._reset_results()
        if df is None or df.empty:
            logger.warning("DataFrame пуст или None, анализ не может быть выполнен.")
            return self.results

        try:
            # 1. Базовая статистика
            self.results['statistics'] = self.get_extended_stats(df)
            
            # 2. Обнаружение выбросов
            self.results['outliers'] = self.detect_all_outliers(df)
            
            # 3. Анализ корреляций
            self.results['correlations'] = self.run_correlation_analysis(df)

            # 4. Анализ временных рядов (если есть дата и цель)
            if date_col and target_col and date_col in df.columns and target_col in df.columns:
                self.analyze_time_series(df, date_col, target_col)
            else:
                self.results['time_series'] = {'success': False, 'message': 'Недостаточно данных (date_col или target_col отсутствуют/неверны).'}

            # 5. Обучение ML модели (если есть цель)
            if target_col and target_col in df.columns:
                self.train_model(df, target_col, model_type=model_type)
            else:
                self.results['ml_model'] = {'success': False, 'message': 'Целевая колонка отсутствует.'}

            # 6. Генерация высокоуровневых выводов (insights)
            self._generate_insights(df)

            # 7. Создание стандартных визуализаций
            try:
                # Используем default_vis_dir, заданный при инициализации
                vis_paths = self.create_plots(df, output_dir=self.default_vis_dir)
                self.results['visualizations'] = vis_paths
            except Exception as e:
                logger.error(f"Ошибка при создании стандартных визуализаций: {e}", exc_info=True)
                self.results['visualizations'] = {'error': str(e)}

        except Exception as e:
            logger.exception(f"Произошла непредвиденная ошибка при анализе данных: {e}")
            self.results['error'] = f"Непредвиденная ошибка: {str(e)}"
            
        # Преобразуем все результаты в нативные Python типы перед возвратом
        return cast(Dict[str, Any], _to_native(self.results))

    def _generate_insights(self, df: pd.DataFrame):
        """
        Генерирует высокоуровневые выводы на основе результатов других анализов.
        """
        insights: List[str] = []

        # 1. Анализ пропусков
        missing_total = int(df.isnull().sum().sum())
        if missing_total > 0:
            # Общее количество ячеек в DataFrame
            total_cells = df.size
            if total_cells > 0:
                missing_perc = (missing_total / total_cells) * 100
                insights.append(f"Обнаружено {missing_total} пропусков ({missing_perc:.2f}% от общего числа ячеек).")
            else:
                insights.append(f"Обнаружено {missing_total} пропусков.")
        else:
            insights.append("Пропуски в данных не обнаружены.")

        # 2. Анализ сильных корреляций
        if self.results.get('correlations'):
            high_corr_pairs = []
            # Проходим по словарю корреляций, который уже _to_native преобразован
            for col1, corr_row in self.results['correlations'].items():
                if not isinstance(corr_row, dict):
                    continue  # Проверка на случай ошибок преобразования
                for col2, val in corr_row.items():
                    # Проверяем, что значение является числом и не является специальным (NaN и т. п.)
                    if isinstance(val, numbers.Real):
                        # Используем math.isnan для скаляров — он работает только с числами
                        if val is not None and not math.isnan(val):
                            corr_value = float(val)
                            # Условие: |corr| > threshold, разные колонки, и пара не дублируется (col1 < col2)
                            if abs(corr_value) > self.high_correlation_threshold and col1 < col2:
                                high_corr_pairs.append(f"'{col1}' и '{col2}' (corr={corr_value:.2f})")

            if high_corr_pairs:
                insights.append(f"Выявлены сильные корреляции (> {self.high_correlation_threshold}): {'; '.join(high_corr_pairs)}.")
            else:
                insights.append(f"Сильные корреляции (> {self.high_correlation_threshold}) между числовыми переменными не обнаружены.")
        else:
            insights.append("Анализ корреляций не выполнен или не дал результатов.")

        # 3. Анализ выбросов
        if 'outliers' in self.results and isinstance(self.results['outliers'], dict):
            outlier_columns_reported = []
            for col, data in self.results['outliers'].items():
                try:
                    # Проверяем, что data — это словарь и содержит ключ 'percentage'
                    if isinstance(data, dict) and 'percentage' in data:
                        percentage = data.get('percentage', 0)
                        # Убеждаемся, что percentage — число
                        if isinstance(percentage, numbers.Real):
                            percentage_float = float(percentage)  # Явное приведение к float
                            if percentage_float > self.outlier_threshold_percentage:
                                count = data.get('count', 'N/A')
                                outlier_columns_reported.append(f"'{col}' ({percentage_float:.1f}%, {count} выбросов)")
                except Exception as e:  # Ловим любые ошибки при доступе к данным выбросов
                    logger.warning(f"Ошибка при анализе отчета о выбросах для колонки '{col}': {e}")

            if outlier_columns_reported:
                insights.append(f"Значительное количество выбросов (> {self.outlier_threshold_percentage}%) обнаружено в колонках: {', '.join(outlier_columns_reported)}.")
            else:
                insights.append(f"Значительное количество выбросов (> {self.outlier_threshold_percentage}%) не обнаружено.")
        else:
            insights.append("Анализ выбросов не выполнен или не дал результатов.")

        self.results['insights'] = insights

    def get_extended_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Вычисляет расширенную статистику для числовых колонок.
        """
        numeric_df = df.select_dtypes(include=[np.number])
        if numeric_df.empty:
            logger.warning("Нет числовых колонок для вычисления расширенной статистики.")
            return {}
            
        stats: Dict[str, Any] = {}
        # Описательная статистика (count, mean, std, min, 25%, 50%, 75%, max)
        try:
            # .describe() возвращает DataFrame, его .to_dict() использует Hashable ключи
            raw_desc = numeric_df.describe().to_dict()
        except Exception as e:
            logger.error(f"Ошибка при вычислении .describe(): {e}", exc_info=True)
            raw_desc = {} # Пустой словарь, если произошла ошибка
        
        # Медианы
        try:
            medians = numeric_df.median().to_dict()
        except Exception as e:
            logger.error(f"Ошибка при вычислении медиан: {e}", exc_info=True)
            medians = {}

        # Проверка на наличие отрицательных значений
        has_negative_flags = (numeric_df < 0).any(axis=0).to_dict()

        # Сбор статистики по колонкам
        for col in numeric_df.columns:
            col_str = str(col) # Ключ должен быть строкой
            col_stats = {}
            
            # Добавляем данные из .describe()
            if col_str in raw_desc:
                col_stats.update({str(k): v for k, v in raw_desc[col_str].items()}) # Преобразуем ключи из describe
            
            # Добавляем медиану
            if col_str in medians:
                col_stats['median'] = medians[col_str]
            
            # Добавляем флаг наличия отрицательных чисел
            col_stats['has_negative'] = bool(has_negative_flags.get(col, False))

            stats[col_str] = col_stats
            
        logger.info(f"Вычислена расширенная статистика для {len(stats)} числовых колонок.")
        return stats

    def detect_all_outliers(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Обнаруживает выбросы в числовых колонках с использованием IQR.
        Возвращает отчет для каждой колонки с количеством и процентом выбросов.
        """
        numeric_cols = df.select_dtypes(include=[np.number])
        outliers_report: Dict[str, Any] = {} # Тип Dict[str, Any] корректен, т.к. значения - словари
        
        if numeric_cols.empty:
            logger.warning("Нет числовых колонок для обнаружения выбросов.")
            return {}

        for col in numeric_cols.columns:
            col_str = str(col)
            # Выбираем только числовые значения, исключая NaN, так как IQR рассчитывается на них
            data = df[col].dropna()
            
            # Проверяем, остались ли данные после dropna()
            if data.empty: 
                outliers_report[col_str] = {'count': 0, 'percentage': 0.0}
                continue
            
            # Расчет квантилей и IQR
            try:
                q1 = data.quantile(0.25)
                q3 = data.quantile(0.75)
                iqr = q3 - q1
                
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                # Подсчет выбросов
                outliers = data[(data < lower_bound) | (data > upper_bound)]
                
                outliers_count = len(outliers)
                total_count = len(data)
                percentage = (outliers_count / total_count * 100) if total_count > 0 else 0.0

                outliers_report[col_str] = {
                    'count': int(outliers_count), # Приводим к int
                    'percentage': float(percentage) # Приводим к float
                }
            except Exception as e:
                logger.error(f"Ошибка при расчете выбросов для колонки '{col_str}': {e}", exc_info=True)
                outliers_report[col_str] = {'error': str(e)}

        logger.info(f"Обнаружение выбросов завершено для {len(outliers_report)} колонок.")
        return outliers_report

    def analyze_time_series(self, df: pd.DataFrame, date_col: str, target_col: str):
        """
        Анализирует временной ряд на стационарность с помощью ADF-теста.
        """
        try:
            # Создаём копию DataFrame, чтобы не изменять оригинал
            ts_df = df[[date_col, target_col]].copy()
            # Преобразование колонки даты, обрабатывая ошибки
            ts_df[date_col] = pd.to_datetime(ts_df[date_col], errors='coerce')
            # Удаляем строки, где дата не распозналась (стала NaT)
            ts_df = ts_df.dropna(subset=[date_col])
            # Устанавливаем дату как индекс и сортируем
            ts_df = ts_df.set_index(date_col).sort_index()
            # Ресемплинг для приведения к ежедневной частоте, заполняя пропуски
            resampled_ts = ts_df[target_col].resample('D').mean().ffill()

            # Проверяем минимальное количество данных
            if len(resampled_ts) < self.min_ts_days:
                raise ValueError(
                    f"Недостаточно данных для анализа временных рядов. Требуется минимум {self.min_ts_days} записей, обнаружено: {len(resampled_ts)}."
                )

            # Проводим ADF-тест
            adf_result = adfuller(resampled_ts)

            # Безопасное извлечение и преобразование значений
            p_value = float(adf_result[1]) if len(adf_result) > 1 else float('nan')
            is_stationary = p_value < 0.05 if not math.isnan(p_value) else False
            adf_statistic = float(adf_result[0]) if len(adf_result) > 0 else float('nan')


            num_obs = -1
            if len(adf_result) > 5:
                try:
                    num_obs_raw = adf_result[5]
                    if isinstance(num_obs_raw, (int, float, np.integer, np.floating)):
                        num_obs = int(float(num_obs_raw))
                    else:
                        num_obs = -1
                except (ValueError, TypeError, OverflowError):
                    num_obs = -1

            self.results['time_series'] = {
                'success': True,
                'is_stationary': is_stationary,
                'p_value': p_value,
                'adf_statistic': adf_statistic,
                'num_obs': num_obs,
                'message': f"Временной ряд {'стационарный' if is_stationary else 'не стационарный'} (p-value={p_value:.4f})."
            }

            # Логирование успешного результата
            logger.info(
                f"Анализ временных рядов завершен. Стационарность: {is_stationary} (p-value: {p_value:.4f}, наблюдений: {num_obs})."
            )

        except ValueError as ve:
            # Обработка специфичных ошибок (например, недостаточно данных)
            error_msg = f"Ошибка анализа временных рядов: {ve}"
            logger.warning(error_msg)
            self.results['time_series'] = {'success': False, 'message': str(ve)}

        except Exception as e:
            # Обработка всех остальных ошибок
            error_msg = f"Произошла ошибка при анализе временных рядов: {e}"
            logger.error(error_msg, exc_info=True)
            self.results['time_series'] = {'success': False, 'error': str(e), 'message': error_msg}

    def train_model(self, df: pd.DataFrame, target_col: str, model_type: ModelType = ModelType.REGRESSION):
        """
        Обучает модель машинного обучения (Random Forest) для регрессии или классификации.
        """
        try:
            # Удаляем строки, где пропущена целевая колонка
            data = df.dropna(subset=[target_col])
            
            # Проверяем достаточность данных
            if len(data) < 20: # Минимальное количество записей для обучения
                raise ValueError("Недостаточно данных (менее 20 записей) для обучения модели.")

            # Разделяем признаки (X) и цель (y)
            X = data.drop(columns=[target_col])
            y = data[target_col]

            # Определяем числовые и категориальные признаки
            num_f = X.select_dtypes(include=[np.number]).columns.tolist()
            cat_f = X.select_dtypes(exclude=[np.number]).columns.tolist()

            # Создаем пайплайн предобработки
            preprocessor = ColumnTransformer(transformers=[
                ('num', Pipeline([('imputer', SimpleImputer(strategy='median')), ('scaler', StandardScaler())]), num_f),
                ('cat', Pipeline([('imputer', SimpleImputer(strategy='most_frequent')), ('onehot', OneHotEncoder(handle_unknown='ignore'))]), cat_f)
            ], remainder='passthrough') # Оставляем остальные колонки, если есть

            # Выбираем модель в зависимости от model_type
            if model_type == ModelType.REGRESSION:
                model = RandomForestRegressor(random_state=self.ml_random_state)
            elif model_type == ModelType.CLASSIFICATION:
                model = RandomForestClassifier(random_state=self.ml_random_state)
            else:
                raise ValueError(f"Неподдерживаемый тип модели: {model_type}")

            # Собираем полный пайплайн ML
            ml_pipeline = Pipeline([('preprocessor', preprocessor), ('model', model)])

            # Разделяем на тренировочную и тестовую выборки
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=self.ml_test_size, random_state=self.ml_random_state, stratify=y if model_type == ModelType.CLASSIFICATION else None)
            
            # Обучаем модель
            ml_pipeline.fit(X_train, y_train)
            
            # Предсказываем на тестовой выборке
            preds = ml_pipeline.predict(X_test)

            # Вычисляем метрики
            metrics = {}
            if model_type == ModelType.REGRESSION:
                metrics['r2'] = float(r2_score(y_test, preds))
                metrics['mse'] = float(mean_squared_error(y_test, preds))
            else: # CLASSIFICATION
                metrics['accuracy'] = float(accuracy_score(y_test, preds))
                metrics['f1_macro'] = float(f1_score(y_test, preds, average='macro', zero_division=0))
                # Добавляем classification_report для более детальной оценки
                try:
                    # report_text = classification_report(y_test, preds, output_dict=True, zero_division=0)
                    # metrics['classification_report'] = report_text # Сохраняем отчет как словарь
                    # Или просто как строку для читаемости
                    metrics['classification_report'] = classification_report(y_test, preds, zero_division=0)
                except Exception as report_err:
                    logger.warning(f"Не удалось сгенерировать classification_report: {report_err}")
                    metrics['classification_report'] = "Ошибка генерации отчета."

            self.results['ml_model'] = {'success': True, 'model_type': model_type.name.lower(), 'metrics': metrics}
            logger.info(f"ML модель ({model_type.name.lower()}) успешно обучена. Метрики: {metrics}")

        except ValueError as ve: # Обработка специфичных ошибок
            logger.warning(f"Ошибка обучения ML модели: {ve}")
            self.results['ml_model'] = {'success': False, 'message': str(ve)}
        except Exception as e:
            logger.error(f"Произошла ошибка при обучении ML модели: {e}", exc_info=True)
            self.results['ml_model'] = {'success': False, 'error': str(e)}

    def run_correlation_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Рассчитывает матрицу корреляций для числовых колонок.
        """
        numeric_df = df.select_dtypes(include=[np.number])
        if numeric_df.empty:
            logger.warning("Нет числовых колонок для анализа корреляций.")
            return {}
        try:
            corr_df = numeric_df.corr()
            # Явно преобразуем все ключи в строки
            corr_dict = {
                str(outer_key): {
                    str(inner_key): value
                    for inner_key, value in inner_dict.items()
                }
                for outer_key, inner_dict in corr_df.to_dict().items()
            }
            logger.info(f"Анализ корреляций завершен для {len(corr_dict)} колонок.")
            return corr_dict
        except Exception as e:
            logger.error(f"Ошибка при расчете корреляций: {e}", exc_info=True)
            return {'error': str(e)}

    def create_plots(self, df: pd.DataFrame, output_dir: str = "reports/visualizations") -> List[str]:
        """
        Генерирует стандартные графики (heatmap корреляций, distribution plot)
        и сохраняет их в указанную директорию.
        """
        out_path = Path(output_dir)
        try:
            out_path.mkdir(parents=True, exist_ok=True)  # Создаём директорию, если она не существует
        except OSError as e:
            logger.error(f"Не удалось создать директорию для визуализаций: {out_path} - {e}", exc_info=True)
            return []  # Возвращаем пустой список при ошибке создания директории

        saved_plots: List[str] = []

        # Проверяем наличие числовых колонок
        numeric_df = df.select_dtypes(include=[np.number])
        if numeric_df.empty:
            logger.warning("Нет числовых колонок для генерации графиков.")
            return []  # Возвращаем пустой список, если нет данных

        # 1. Heatmap корреляций
        if len(numeric_df.columns) > 1:  # Heatmap имеет смысл, если колонок больше одной
            try:
                plt.figure(figsize=(10, 8))  # Создаём новую фигуру
                sns.heatmap(numeric_df.corr(), annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5)
                plt.title("Матрица корреляций")

                plot_path_corr = str(out_path / "correlation_heatmap.png")
                plt.savefig(plot_path_corr, bbox_inches='tight')  # Сохраняем с подгонкой границ
                plt.close()  # Закрываем фигуру, чтобы освободить память
                saved_plots.append(plot_path_corr)
                logger.info(f"Heatmap корреляций сохранён: {plot_path_corr}")
            except Exception as e:
                logger.error(f"Ошибка при генерации heatmap корреляций: {e}", exc_info=True)
                # Не добавляем в saved_plots, просто логируем ошибку
        else:
            logger.info("Heatmap корреляций пропущен (нужно >1 числовой колонки).")

        # 2. Distribution plot (гистограмма с KDE) для первой числовой колонки
        try:
            col_to_plot = str(numeric_df.columns[0])  # Берём первую числовую колонку
            plt.figure(figsize=(8, 6))
            sns.histplot(data=numeric_df, x=col_to_plot, kde=True)
            plt.title(f"Распределение: {col_to_plot}")

            plot_path_dist = str(out_path / "distribution_histplot.png")
            plt.savefig(plot_path_dist, bbox_inches='tight')
            plt.close()
            saved_plots.append(plot_path_dist)
            logger.info(f"Distribution plot сохранён: {plot_path_dist}")
        except Exception as e:
            logger.error(f"Ошибка при генерации distribution plot: {e}", exc_info=True)
            # Не добавляем в saved_plots, просто логируем ошибку

        return saved_plots

