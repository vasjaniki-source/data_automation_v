import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, Optional, List

# ML Tools
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    mean_squared_error, r2_score,
    accuracy_score, f1_score
)

# Stats & Time Series
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller

import os
from pathlib import Path

logger = logging.getLogger(__name__)

class DataAnalyzer:
    """
    Класс для продвинутого анализа данных и ML.
    """
    def __init__(self):
        self.results: Dict[str, Any] = {}
        self._reset_results()

    def _reset_results(self):
        """Инициализация структуры результатов."""
        self.results = {
            'statistics': {},
            'outliers': {},
            'time_series': {'success': False},
            'ml_model': {'success': False, 'metrics': {}},
            'correlations': {},
            'visualizations': {}
        }

    def analyze(self, df: pd.DataFrame, target_col: Optional[str] = None, 
                date_col: Optional[str] = None, model_type: str = 'regression') -> Dict[str, Any]:
        """Комплексный запуск всех видов анализа."""
        self._reset_results()
        
        # 1. Базовая статистика
        self.results['statistics'] = self.get_extended_stats(df)
        
        # 2. Поиск аномалий
        self.results['outliers'] = self.detect_all_outliers(df)
        
        # 3. Корреляции
        self.results['correlations'] = self.run_correlation_analysis(df)
        
        # 4. Временные ряды
        if date_col and target_col:
            self.analyze_time_series(df, date_col, target_col)
            
        # 5. ML Моделирование
        if target_col:
            self.train_model(df, target_col, model_type)
            
        return self.results

    def run_selective_analysis(self, df: pd.DataFrame, modules: Optional[List[str]] = None, **kwargs) -> Dict[str, Any]:
        """
        Выполняет выборочный анализ. modules - список: 'statistics', 'outliers', 'correlations', 'time_series', 'ml'
        Возвращает словарь с результатами только запрошенных модулей.
        """
        if modules is None:
            modules = ['statistics', 'outliers', 'correlations']

        results: Dict[str, Any] = {}
        try:
            if 'statistics' in modules:
                results['statistics'] = self.get_extended_stats(df)
            if 'outliers' in modules:
                results['outliers'] = self.detect_all_outliers(df)
            if 'correlations' in modules:
                results['correlations'] = self.run_correlation_analysis(df)
            if 'time_series' in modules:
                date_col = kwargs.get('date_col')
                target_col = kwargs.get('target_col')
                if date_col and target_col:
                    # Выполняем временной анализ, помещая результат во временный анализатор
                    self._reset_results()
                    self.analyze_time_series(df, date_col, target_col)
                    results['time_series'] = self.results.get('time_series', {})
                else:
                    results['time_series'] = {'success': False, 'error': 'date_col and target_col required'}
            if 'ml' in modules:
                target_col = kwargs.get('target_col')
                model_type = kwargs.get('model_type', 'regression')
                if target_col:
                    self._reset_results()
                    self.train_model(df, target_col, model_type)
                    results['ml_model'] = self.results.get('ml_model', {})
                else:
                    results['ml_model'] = {'success': False, 'error': 'target_col required'}
        except Exception as e:
            logger.error(f"Ошибка в выборочном анализе: {e}")
            results['error'] = str(e)
        return results

    def get_extended_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Расчет статистик. Приведение ключей к str."""
        numeric_df = df.select_dtypes(include=[np.number])
        if numeric_df.empty:
            return {}
        # Используем describe + дополнительные метрики
        raw_stats = numeric_df.agg(['mean', 'median', 'std', 'skew', 'kurtosis']).to_dict()
        stats: Dict[str, Any] = {str(col): values for col, values in raw_stats.items()}
        
        for col in numeric_df.columns:
            mode_val = df[col].mode()
            col_str = str(col)
            if col_str not in stats:
                stats[col_str] = {}
            stats[col_str]['mode'] = mode_val[0] if not mode_val.empty else None
        return stats

    def detect_all_outliers(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Выявление выбросов."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        outliers_report: Dict[str, Any] = {}

        for col in numeric_cols:
            data = df[col].dropna()
            if data.empty:
                outliers_report[str(col)] = {'iqr_count': 0, 'z_count': 0, 'total_indices': []}
                continue
            
            q1, q3 = data.quantile(0.25), data.quantile(0.75)
            iqr = q3 - q1
            iqr_outliers = data[(data < (q1 - 1.5 * iqr)) | (data > (q3 + 1.5 * iqr))]
            
            std_val = data.std()
            z_scores = np.abs((data - data.mean()) / (std_val if std_val > 0 else 1e-9))
            z_outliers = data[z_scores > 3]
            
            outliers_report[str(col)] = {
                'iqr_count': int(len(iqr_outliers)),
                'z_count': int(len(z_outliers)),
                'total_indices': list(sorted(set(iqr_outliers.index) | set(z_outliers.index)))
            }
        return outliers_report

    def analyze_time_series(self, df: pd.DataFrame, date_col: str, target_col: str):
        """Анализ временных рядов."""
        try:
            ts_df = df[[date_col, target_col]].copy()
            ts_df[date_col] = pd.to_datetime(ts_df[date_col])
            ts_df = ts_df.set_index(date_col).sort_index()
            
            # Ресемплирование по дням и заполнение
            ts_df = ts_df.resample('D').mean().ffill()
            
            if len(ts_df) < 14:
                raise ValueError("Недостаточно данных для временного анализа (нужно >=14).")

            decomposition = seasonal_decompose(ts_df[target_col], model='additive', period=7)
            dftest = adfuller(ts_df[target_col].dropna())
            
            p_value = float(dftest[1])
            
            self.results['time_series'] = {
                'success': True,
                'is_stationary': p_value < 0.05,
                'p_value': round(p_value, 4),
                'trend_last': float(decomposition.trend.dropna().iloc[-1]) if not decomposition.trend.dropna().empty else 0.0,
                'seasonal_strength': float(decomposition.seasonal.std())
            }
        except Exception as e:
            logger.error(f"Ошибка временного ряда: {e}")
            self.results['time_series'] = {'success': False, 'error': str(e)}

    def train_model(self, df: pd.DataFrame, target_col: str, model_type: str = 'regression'):
        """Построение ML моделей."""
        try:
            # Очистка данных от NaN в целевой колонке
            clean_df = df.dropna(subset=[target_col])
            X = clean_df.drop(columns=[target_col])
            y = clean_df[target_col]
            
            if X.shape[0] < 5:
                raise ValueError("Недостаточно строк для обучения модели (минимум 5).")

            num_cols = X.select_dtypes(include=[np.number]).columns.tolist()
            cat_cols = X.select_dtypes(exclude=[np.number]).columns.tolist()

            numeric_transformer = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='median')),
                ('scaler', StandardScaler())
            ])
            categorical_transformer = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='most_frequent')),
                ('onehot', OneHotEncoder(handle_unknown='ignore'))
            ])
            
            preprocessor = ColumnTransformer(transformers=[
                ('num', numeric_transformer, num_cols),
                ('cat', categorical_transformer, cat_cols)
            ])

            model = LinearRegression() if model_type == 'regression' else RandomForestClassifier(n_estimators=100)
            clf = Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            clf.fit(X_train, y_train)
            y_pred = clf.predict(X_test)

            metrics: Dict[str, float] = {}
            if model_type == 'regression':
                metrics['rmse'] = float(np.sqrt(mean_squared_error(y_test, y_pred)))
                metrics['r2'] = float(r2_score(y_test, y_pred))
            else:
                metrics['accuracy'] = float(accuracy_score(y_test, y_pred))
                metrics['f1'] = float(f1_score(y_test, y_pred, average='weighted'))

            self.results['ml_model'] = {
                'success': True,
                'model_type': model_type,
                'metrics': metrics
            }
        except Exception as e:
            logger.error(f"Ошибка ML: {e}")
            self.results['ml_model'] = {'success': False, 'error': str(e)}

    def run_correlation_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Корреляционный анализ. Приведение ключей к str."""
        numeric = df.select_dtypes(include=[np.number])
        if numeric.empty:
            return {}
        corr_matrix = numeric.corr()
        return {str(k): v for k, v in corr_matrix.to_dict().items()}

    def create_plots(self, df: pd.DataFrame, output_dir: str = "reports/visualizations") -> List[str]:
        """Генерация графиков и возврат сохранённых путей."""
        os.makedirs(output_dir, exist_ok=True)
        saved_paths: List[str] = []
        
        # 1. Корреляции
        numeric = df.select_dtypes(include=[np.number])
        if not numeric.empty:
            try:
                plt.figure(figsize=(10, 8))
                sns.heatmap(numeric.corr(), annot=True, cmap='coolwarm')
                path_corr = str(Path(output_dir) / "correlation_matrix.png")
                plt.savefig(path_corr, bbox_inches='tight')
                plt.close()
                saved_paths.append(path_corr)
            except Exception as e:
                logger.error(f"Ошибка при создании heatmap: {e}")

        # 2. Гистограмма первой числовой колонки (если есть)
        if not numeric.empty:
            try:
                plt.figure(figsize=(8, 6))
                col_name = numeric.columns[0]
                sns.histplot(data=df, x=col_name, kde=True)
                plt.title(f"Distribution of {col_name}")
                path_hist = str(Path(output_dir) / "target_distribution.png")
                plt.savefig(path_hist, bbox_inches='tight')
                plt.close()
                saved_paths.append(path_hist)
            except Exception as e:
                logger.error(f"Ошибка при создании гистограммы: {e}")

        return saved_paths

    def get_report_data(self) -> Dict[str, Any]:
        return self.results
