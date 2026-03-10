import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, numeric_cols: Optional[list] = None, date_cols: Optional[list] = None):
        self.numeric_cols = numeric_cols or ['id', 'age', 'salary']
        self.date_cols = date_cols or ['join_date']
        self.logger = logging.getLogger(__name__)
        self.logger.info("DataValidator инициализирован")

    def validate(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any], pd.DataFrame]:
        self.logger.info("Начата валидация DataFrame")
        work_df = df.copy() # Работаем с копией, чтобы не изменять оригинальный df

        # --- ПОДСЧЕТ ПРОБЛЕМ ДО ОЧИСТКИ ---
        initial_missing_count = int(work_df.isnull().sum().sum())
        initial_duplicate_count = int(work_df.duplicated().sum())

        results = {
            'validation_status': 'SUCCESS',
            'issues_found': False,
            'initial_rows': len(work_df),
            'total_missing': initial_missing_count,
            'duplicates': initial_duplicate_count
        }

        # --- ВЫПОЛНЕНИЕ ОЧИСТКИ ---
        # 1. Приведение типов
        self._convert_data_types(work_df)

        # 2. Обработка дубликатов  
        self._handle_duplicates(work_df) 
        
        # 3. Обработка пропусков  
        self._handle_missing_values(work_df) 

        # 4. Выявление и обработка выбросов  
        self._detect_and_handle_outliers(work_df)

        # 5. Проверка целостности
        integrity_results = self._check_data_integrity(work_df)
        results['integrity_check'] = integrity_results

        # --- ОБНОВЛЕНИЕ СТАТУСА НА ОСНОВЕ ИСХОДНЫХ ПРОБЛЕМ ---
        # Статус "WARNING" ставится, если были проблемы в ИСХОДНЫХ данных
        if initial_duplicate_count > 0 or initial_missing_count > 0 or integrity_results.get('has_errors', False):
            results['validation_status'] = 'WARNING'
            results['issues_found'] = True
        
        results['final_rows'] = len(work_df)
        
        is_valid = not results['issues_found'] 
        
        return is_valid, results, work_df 
  
    def _convert_data_types(self, df: pd.DataFrame):
        for col in self.numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                self.logger.debug(f"Колонка '{col}' преобразована в числовой тип. Dtype: {df[col].dtype}")
        for col in self.date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                self.logger.debug(f"Колонка '{col}' преобразована в дату. Dtype: {df[col].dtype}")
                
    def _handle_duplicates(self, df: pd.DataFrame):
        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        if len(df) < initial_rows:
            self.logger.info(f"Удалено {initial_rows - len(df)} дубликатов.")

    def _handle_missing_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Обрабатывает пропущенные значения:
        - Для числовых колонок заполняет медианой (если медиана существует, иначе 0 или удаляет).
        - Для остальных колонок - удаляет строки.
        """
        initial_rows = len(df)
        missing_cols = df.columns[df.isnull().any()].tolist()
        
        if not missing_cols:
            self.logger.info("Пропусков не обнаружено.")
            return {}

        rows_to_drop = set() 

        for col in missing_cols:
            self.logger.debug(f"Обработка пропусков в колонке: '{col}'. Изначально пропусков: {df[col].isnull().sum()}")
            if pd.api.types.is_numeric_dtype(df[col]):
                # Заполняем числовые колонки медианой
                numeric_values = df[col].dropna()
                if not numeric_values.empty:
                    median_val = numeric_values.median()
                    df.loc[:, col] = df[col].fillna(median_val)
                    self.logger.info(f"Заполнены пропуски в числовой колонке '{col}' медианой: {median_val}")
                else:
                    # Если вся колонка числовая, но содержит только NaN, 
                    # заполняем 0 или средним, или удаляем строки (выбираем стратегию)
                    # Для надежности в тестах, давайте заполним 0, если нет медианы
                    df[col].fillna(0, inplace=True) 
                    self.logger.warning(f"Колонка '{col}' числовая, но полностью состоит из NaN/пустая. Заполнено 0.")
            else:
                # Для нечисловых колонок - собираем индексы строк для удаления
                rows_to_drop.update(df[df[col].isnull()].index.tolist())
                self.logger.info(f"Найдены строки с пропусками в нечисловой колонке '{col}'.")
            self.logger.debug(f"Пропусков в '{col}' после обработки: {df[col].isnull().sum()}")
        
        # Удаляем собранные строки (для нечисловых колонок или полностью NaN числовых)
        if rows_to_drop:
            df.drop(index=list(rows_to_drop), inplace=True)
            self.logger.info(f"Удалено {len(rows_to_drop)} строк с пропусками.")
        
        self.logger.debug(f"Общее количество пропусков после _handle_missing_values: {df.isnull().sum().sum()}")
        
        if len(df) < initial_rows:
            self.logger.info(f"Общее количество строк уменьшилось на {initial_rows - len(df)} после обработки пропусков.")
        return {}

    def _detect_and_handle_outliers(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Поиск и обработка выбросов с использованием Z-score и IQR"""
        numeric_cols_in_df = df.select_dtypes(include=[np.number]).columns
        outliers_zscore = {}
        outliers_iqr = {}

        for col in numeric_cols_in_df:
            valid_data = df[col].dropna()
            if len(valid_data) > 0 and valid_data.std() != 0:
                # Z-score
                z_scores = np.abs((valid_data - valid_data.mean()) / valid_data.std())
                z_outliers = (z_scores > 3).sum()
                outliers_zscore[col] = int(z_outliers)

                # IQR
                Q1 = valid_data.quantile(0.25)
                Q3 = valid_data.quantile(0.75)
                IQR = Q3 - Q1
                iqr_outliers = ((valid_data < (Q1 - 1.5 * IQR)) | (valid_data > (Q3 + 1.5 * IQR))).sum()
                outliers_iqr[col] = int(iqr_outliers)

                # Замена выбросов на медиану
                if z_outliers > 0:
                    median_val = df[col].median()
                    z_mask = np.abs((df[col] - df[col].mean()) / df[col].std()) > 3
                    df.loc[z_mask, col] = median_val
                    self.logger.info(f"Выбросы в '{col}' заменены на медиану: {median_val}")

        total_outliers = sum(outliers_zscore.values()) + sum(outliers_iqr.values())

        return {
            'outliers_zscore': outliers_zscore,
            'outliers_iqr': outliers_iqr,
            'total_outliers': int(total_outliers)
        }
    
    def _check_data_integrity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Дополнительная проверка целостности данных."""
        
        # ИСПРАВЛЕНИЕ: Создаем словарь для результатов, а не возвращаем заглушку
        check_results = {
            'has_errors': False,
            'details': []
        }

        # Проверка на отрицательные значения в числовых столбцах, где это нелогично
        for col in df.select_dtypes(include=[np.number]).columns:
            # Проверяем, есть ли столбец в списке числовых, которые мы ожидаем
            # Или просто игнорируем колонки, где отрицательные значения нелогичны
            # Пример: 'age' не должен быть отрицательным
            if col.lower() in ['age', 'id'] and not df[col].isnull().all(): # Проверяем, если столбец не пустой
                min_val = df[col].min()
                if pd.notna(min_val) and min_val < 0:
                    # ИСПРАВЛЕНИЕ: Добавляем в список 'details', а не просто текст
                    check_results['details'].append(
                        f"Отрицательные значения в столбце '{col}': {min_val}"
                    )
                    check_results['has_errors'] = True # Устанавливаем флаг, если найдена ошибка

        # Проверка диапазонов дат
        for col in self.date_cols:
            if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                date_series = df[col].dropna()
                if len(date_series) > 0:
                    min_date = date_series.min()
                    max_date = date_series.max()
                    # ИСПРАВЛЕНИЕ: Используем более разумные границы, например, 1900-2100
                    if min_date.year < 1900 or max_date.year > 2100:
                        check_results['details'].append(
                            f"Аномальные даты в столбце '{col}': "
                            f"{min_date.strftime('%Y-%m-%d')} — {max_date.strftime('%Y-%m-%d')}"
                        )
                        check_results['has_errors'] = True

        # Проверка категориальных столбцов на доминирующие значения
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_cols:
            # ИСПРАВЛЕНИЕ: Убедимся, что столбец не пустой и есть чем считать
            if col in df.columns and not df[col].dropna().empty:
                value_counts = df[col].value_counts(normalize=True, dropna=True)
                if len(value_counts) > 0 and value_counts.iloc[0] > 0.95:
                    most_common = value_counts.index[0]
                    check_results['details'].append(
                        f"Столбец '{col}' содержит доминирующее значение '{most_common}' "
                        f"({value_counts.iloc[0]:.1%} данных)"
                    )
                    check_results['has_errors'] = True

        return check_results
    
    def get_validation_report(self, results: Dict[str, Any]) -> str:
        """Форматирование отчёта о валидации для вывода/сохранения"""
        report = ["\n" + "="*50]
        report.append("ОТЧЁТ О ВАЛИДАЦИИ ДАННЫХ")
        report.append("="*50)

        validation_status = results.get('validation_status', 'UNKNOWN')
        report.append(f"\nСтатус: {validation_status}")

        cleaned_df = results.get('cleaned_df')
        if cleaned_df is not None:
            report.append(f"Всего строк: {len(cleaned_df)}")
        else:
            report.append("Данные очищенного DataFrame недоступны")

        # 1. Дубликаты
        report.append(f"\n1. ДУБЛИКАТЫ")
        duplicates = results.get('duplicates', 0)
        report.append(f"   Найдено и удалено: {duplicates}")

        # 2. Пропуски
        report.append(f"\n2. ПРОПУСКИ")
        total_missing = results.get('total_missing', 0)
        final_missing = results.get('final_missing', 0)
        report.append(f"   Всего пропусков: {total_missing}")
        report.append(f"   Осталось после обработки: {final_missing}")

        # 3. Типы данных
        report.append(f"\n3. ТИПЫ ДАННЫХ")
        data_types = results.get('data_types', {})
        if data_types:
            for dtype, count in data_types.items():
                report.append(f"   {dtype}: {count}")
        else:
            report.append("   Информация о типах данных недоступна")

        # 4. Выбросы
        report.append(f"\n4. ВЫБРОСЫ")
        zscore_outliers = results.get('outliers_zscore', {})
        iqr_outliers = results.get('outliers_iqr', {})

        if zscore_outliers:
            report.append(f"   Z-score: {dict(zscore_outliers)}")
        else:
            report.append("   Z-score: нет данных")

        if iqr_outliers:
            report.append(f"   IQR: {dict(iqr_outliers)}")
        else:
            report.append("   IQR: нет данных")

        total_outliers = results.get('total_outliers', 0)
        report.append(f"   Всего обработано: {total_outliers}")

        # 5. Проблемы целостности
        integrity_check = results.get('integrity_check', {})
        if integrity_check and integrity_check['issues']:
            report.append(f"\n5. ПРОБЛЕМЫ ЦЕЛОСТНОСТИ")
            for issue in integrity_check['issues']:
                report.append(f"   • {issue}")
        else:
            report.append(f"\n5. ПРОБЛЕМЫ ЦЕЛОСТНОСТИ: не обнаружены")

        report.append("\n" + "-"*50)
        return "\n".join(report)