from dataclasses import dataclass
import logging
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

@dataclass
class CleaningResult:
    """
    Контейнер для результатов очистки данных.

    Атрибуты:
    -----------
    data : pd.DataFrame
        Очищенные данные в формате DataFrame.
    report : Dict[str, Any]
        Подробный отчёт о процессе очистки, включая метаданные,
        информацию о качестве данных и сводку шагов обработки.
    """
    data: pd.DataFrame
    report: Dict[str, Any]

class DataCleaner:
    def __init__(self,
                 numeric_cols: Optional[List[str]] = None,
                 categorical_cols: Optional[List[str]] = None,
                 date_cols: Optional[List[str]] = None,
                 target_col: Optional[str] = None):
        """Инициализация очистителя данных."""
        self.numeric_cols = numeric_cols or []
        self.categorical_cols = categorical_cols or []
        self.date_cols = date_cols or []
        self.target_col = target_col
        self.label_encoders = {}
        self.scaler = StandardScaler()
        self.logger = logging.getLogger(__name__)
        self.logger.info("DataCleaner инициализирован")


    def clean(self, df: pd.DataFrame) -> CleaningResult:
        """Комплексная очистка данных с логированием каждого этапа."""
        self.logger.info("Начата очистка данных")

        # Инициализация структуры отчёта
        results = {
            'metadata': {
                'original_shape': df.shape,
                'processing_started': True
            },
            'data_quality': {},
            'processing_steps': {},
            'final_summary': {}
        }

        cleaned_df = df.copy()

        # 1. Удаление дубликатов
        results['data_quality']['duplicates'] = self._remove_duplicates(cleaned_df)

        # 2. Обработка пропущенных значений
        missing_info = self._handle_missing_values(cleaned_df)
        results['data_quality'].update(missing_info)

        # 3. Преобразование дат
        if self.date_cols:
            date_conversion = self._convert_dates(cleaned_df)
            results['processing_steps']['date_conversion'] = date_conversion


        # 4. Кодирование категориальных признаков
        if self.categorical_cols:
            encoding_results = self._encode_categorical(cleaned_df)
            results['processing_steps']['encoding'] = encoding_results

        # 5. Масштабирование числовых признаков
        if self.numeric_cols:
            scaling_results = self._scale_numeric(cleaned_df)
            results['processing_steps']['scaling'] = scaling_results

        # Финальная сводка
        self._generate_final_summary(results, df, cleaned_df)

        self.logger.info(f"Очистка данных завершена. Статус: {results['final_summary']['overall_status']}")

        # Возвращаем структурированный результат
        return CleaningResult(data=cleaned_df, report=results)

    def _remove_duplicates(self, df: pd.DataFrame) -> Dict[str, int]:
        """Удаление дубликатов с логированием."""
        duplicates_before = df.duplicated().sum()
        df.drop_duplicates(inplace=True)
        duplicates_removed = duplicates_before - df.duplicated().sum()

        result = {
            'before': int(duplicates_before),
            'after': int(df.duplicated().sum()),
            'removed': int(duplicates_removed),
            'percentage_reduction': round(
                (duplicates_removed / duplicates_before * 100)
                if duplicates_before > 0 else 0
            )
        }

        if duplicates_removed > 0:
            self.logger.warning(f"Удалено {duplicates_removed} дубликатов")
        else:
            self.logger.info("Дубликаты не обнаружены")

        return result

    def _handle_missing_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Обработка пропущенных значений."""
        missing_before = df.isnull().sum().sum()
        columns_with_missing = df.columns[df.isnull().any()].tolist()

        for col in columns_with_missing:
            if col == self.target_col:
                continue

            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                if col in self.numeric_cols:
                    fill_value = df[col].median()
                    df[col].fillna(fill_value, inplace=True)
                    self.logger.info(f"Заполнены пропуски в '{col}' медианой: {fill_value}")
                elif col in self.categorical_cols:
                    mode_vals = df[col].mode()
                    if len(mode_vals) > 0:
                        fill_value = mode_vals[0]
                    else:
                        fill_value = 'UNKNOWN'
                    df[col].fillna(fill_value, inplace=True)
                    self.logger.info(f"Заполнены пропуски в '{col}' значением: {fill_value}")
            else:
                # Для остальных столбцов
                if pd.api.types.is_numeric_dtype(df[col]):
                    fill_value = df[col].median()
                else:
                    mode_vals = df[col].mode()
                    fill_value = mode_vals[0] if len(mode_vals) > 0 else 'MISSING'
                    df[col].fillna(fill_value, inplace=True)

        missing_after = df.isnull().sum().sum()

        return {
            'missing_values': {
                'before': int(missing_before),
                'after': int(missing_after),
                'filled': int(missing_before - missing_after),
                'percentage_reduction': round(
                    ((missing_before - missing_after) / missing_before * 100)
                    if missing_before > 0 else 0),
                'columns_affected': len(columns_with_missing)
            }
        }

    def _convert_dates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Преобразование строковых дат в datetime и извлечение компонентов."""
        result = {'status': 'SUCCESS', 'processed_columns': [], 'new_columns_created': []}

        for col in self.date_cols:
            if col in df.columns:
                try:
                    # Преобразуем в datetime с помощью pd.to_datetime
                    # errors='coerce' преобразует невалидные даты в NaT (NaT = Not a Time)
                    df[col] = pd.to_datetime(df[col], errors='coerce')

                    # Проверяем, что столбец действительно стал datetime
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        # Создаём DatetimeIndex для удобного извлечения компонентов
                        datetime_index = pd.DatetimeIndex(df[col], copy=False)

                        # Извлекаем компоненты даты и создаём новые столбцы
                        df[f'{col}_year'] = datetime_index.year
                        df[f'{col}_month'] = datetime_index.month
                        df[f'{col}_day'] = datetime_index.day
                        df[f'{col}_dayofweek'] = datetime_index.dayofweek
                        df[f'{col}_quarter'] = datetime_index.quarter
                        df[f'{col}_weekofyear'] = datetime_index.isocalendar().week

                        # Обновляем список обработанных и созданных столбцов
                        new_columns = [
                            f'{col}_{part}'
                            for part in ['year', 'month', 'day', 'dayofweek', 'quarter', 'weekofyear']
                        ]
                        result['processed_columns'].append(col)
                        result['new_columns_created'].extend(new_columns)

                        self.logger.info(f"Дата преобразована в '{col}', созданы новые столбцы: {new_columns}")
                    else:
                        error_msg = f"Не удалось преобразовать столбец '{col}' в формат datetime"
                        result = {'status': 'ERROR', 'error_message': error_msg}
                        self.logger.error(error_msg)
                        break

                except Exception as e:
                    error_msg = f"Ошибка преобразования даты в столбце '{col}': {e}"
                    result = {'status': 'ERROR', 'error_message': error_msg}
                    self.logger.error(error_msg)
                    break

        return result

    def _encode_categorical(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Кодирование категориальных признаков."""
        encoding_results = {'onehot': {}, 'label': {}}

        # One-hot кодирование для столбцов с небольшим числом уникальных значений
        onehot_cols = [
            col for col in self.categorical_cols
            if col in df.columns and df[col].nunique() <= 10
        ]

        if onehot_cols:
            self.logger.info(f"One-hot кодирование для столбцов: {onehot_cols}")
            for col in onehot_cols:
                # Заполняем пропуски специальной меткой перед one-hot
                df[col] = df[col].fillna('MISSING_CATEGORY')
                onehot_encoded = pd.get_dummies(
                    df[col],
            prefix=col,
            dummy_na=False  # Пропуски уже заполнены
        )
                # Удаляем оригинальный столбец и добавляем one-hot закодированные
                df.drop(columns=[col], inplace=True)
                df = pd.concat([df, onehot_encoded], axis=1)

                encoding_results['onehot'][col] = {
                    'unique_values_count': int(df[col].dropna().nunique()),
            'new_columns': list(onehot_encoded.columns),
            'total_samples': len(df),
            'missing_filled': int(df[col].isna().sum())
        }

        # Label encoding для остальных категориальных столбцов
        label_cols = [
            col for col in self.categorical_cols
            if col in df.columns and col not in onehot_cols
        ]

        for col in label_cols:
            self.logger.info(f"Label encoding для '{col}'")
            le = LabelEncoder()
            non_null_mask = df[col].notna()

            if non_null_mask.any():
                # Заполняем пропуски временной меткой для обучения кодировщика
                temp_fill = f'__MISSING_{col}__'
                temp_series = df[col].fillna(temp_fill)
                transformed_values = le.fit_transform(temp_series)

                # Заменяем метку пропуска на NaN в итоговом результате
                transformed_array = np.where(
                    temp_series == temp_fill,
            np.nan,
            transformed_values
        )
                df[col] = transformed_array
                self.label_encoders[col] = le

                encoding_results['label'][col] = {
            'classes_count': int(len(le.classes_)),
            'encoded_samples': int(non_null_mask.sum()),
            'missing_filled': int(df[col].isna().sum())
        }
            else:
                # Если все значения NaN — заполняем нулями
                df[col] = 0.0
                self.label_encoders[col] = le
                encoding_results['label'][col] = {
            'classes_count': 0,
            'encoded_samples': 0,
            'missing_filled': len(df)
        }

        return encoding_results

    def _scale_numeric(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Масштабирование числовых признаков."""
        scaling_results = {}

        # Проверка наличия числовых столбцов в DataFrame
        available_numeric = [col for col in self.numeric_cols if col in df.columns]
        missing_numeric = [col for col in self.numeric_cols if col not in df.columns]

        if not available_numeric:
            skip_details = {
                'method': 'StandardScaler',
            'status': 'SKIPPED',
            'message': 'No numeric columns available in DataFrame',
            'requested_columns': self.numeric_cols,
            'missing_columns': missing_numeric
        }
            self.logger.warning("Масштабирование пропущено — отсутствуют числовые столбцы")
            return skip_details

        self.logger.info(f"Масштабирование числовых признаков: {available_numeric}")

        numeric_data = df[available_numeric].copy()

        # Приводим типы данных к float64 перед масштабированием
        for col in available_numeric:
            if df[col].dtype != 'float64':
                self.logger.debug(f"Приведение типа столбца '{col}' к float64")
                df[col] = df[col].astype('float64')
                numeric_data[col] = numeric_data[col].astype('float64')

        # Создаём маску для строк без пропусков во всех числовых столбцах
        non_null_mask = numeric_data.notna().all(axis=1)
        numeric_data_clean = numeric_data[non_null_mask]

        if len(numeric_data_clean) > 0:
            try:
                # Дополнительная проверка: все ли столбцы имеют ненулевое стандартное отклонение
                zero_std_cols = numeric_data_clean.columns[numeric_data_clean.std() == 0].tolist()
                if zero_std_cols:
                    self.logger.warning(f"Пропуск масштабирования — константные столбцы: {zero_std_cols}")
                    # Удаляем константные столбцы из данных для масштабирования
                    numeric_data_clean = numeric_data_clean.drop(columns=zero_std_cols)
                    available_numeric = [col for col in available_numeric if col not in zero_std_cols]

                if numeric_data_clean.empty:
                    warning_details = {
                        'method': 'StandardScaler',
                        'status': 'WARNING',
                        'message': 'Все числовые столбцы константны, масштабирование невозможно',
                        'columns': available_numeric
                    }
                    self.logger.warning("Все числовые столбцы константны, масштабирование пропущено")
                    return warning_details

                # Масштабируем данные
                scaled_data_clean = self.scaler.fit_transform(numeric_data_clean)

                # Вставляем масштабированные значения обратно, сохраняя индексы
                df.loc[non_null_mask, available_numeric] = scaled_data_clean

                scaling_results = {
                'method': 'StandardScaler',
                'columns': available_numeric,
                'missing_columns_skipped': missing_numeric,
                'zero_std_columns_skipped': zero_std_cols,
                'mean_before': numeric_data_clean.mean().to_dict(),
                'std_before': numeric_data_clean.std().to_dict(),
                'samples_used_for_scaling': int(len(numeric_data_clean)),
                'scaling_applied': True
                }
                self.logger.info("Масштабирование выполнено успешно")
            except Exception as e:
                error_details = {
                'method': 'StandardScaler',
                'status': 'ERROR',
                'error_message': str(e),
                'problematic_columns': available_numeric
                }
                self.logger.error(f"Ошибка масштабирования числовых признаков: {e}")
                return error_details
        else:
            warning_details = {
                'method': 'StandardScaler',
                'status': 'WARNING',
                'message': 'No data available for scaling (all NaN in numeric columns)',
                'columns': available_numeric
            }
            self.logger.warning("Недостаточно данных для масштабирования (все значения NaN в числовых столбцах)")
            return warning_details

        return scaling_results

    def _generate_final_summary(self, results: Dict[str, Any], original_df: pd.DataFrame, cleaned_df: pd.DataFrame):
        """Генерация финальной сводки по очистке данных."""
        missing_after = cleaned_df.isnull().sum().sum()
        original_rows = original_df.shape[0]
        final_rows = cleaned_df.shape[0]

        # Безопасный расчёт процента удалённых строк
        if original_rows > 0:
            rows_removed_percentage = round(((original_rows - final_rows) / original_rows * 100), 2)
        else:
            rows_removed_percentage = 0.0

        results['final_summary'] = {
            'original_rows': int(original_rows),
            'final_rows': int(final_rows),
            'total_columns': int(cleaned_df.shape[1]),
            'rows_removed_percentage': rows_removed_percentage,
            'missing_values_after_cleaning': int(missing_after),
            'overall_status': 'SUCCESS' if missing_after == 0 else 'WARNING',
            'processing_completed': True
        }

        # Добавляем информацию о типах данных финального DataFrame
        dtype_info = cleaned_df.dtypes.value_counts().to_dict()  
        results['final_summary']['data_types'] = {
            str(k): int(v) for k, v in dtype_info.items()
        }

        # Дополнительная статистика по пропущенным значениям
        missing_per_column = cleaned_df.isnull().sum()
        columns_with_missing = missing_per_column[missing_per_column > 0]
        results['final_summary']['columns_with_missing_values'] = {
            col: int(count) for col, count in columns_with_missing.items()
        }

        # Логируем финальную сводку
        self.logger.info(
            f"Очистка завершена: {results['final_summary']['original_rows']} → "
            f"{results['final_summary']['final_rows']} строк, "
            f"{results['final_summary']['total_columns']} столбцов, "
            f"удалено {rows_removed_percentage}% строк"
        )

        if results['final_summary']['overall_status'] == 'WARNING':
            self.logger.warning(
                f"Обнаружены пропущенные значения после очистки: {missing_after} "
                f"в столбцах: {list(columns_with_missing.index)}"
            )
            # Детализация по столбцам с пропусками
            for col, count in columns_with_missing.items():
                self.logger.debug(f"Столбец '{col}': {count} пропущенных значений")
        else:
            self.logger.info("Все этапы очистки завершены успешно, пропусков нет")

        return results  

    def get_feature_importance_mask(self, df: pd.DataFrame, threshold: float = 0.01, min_unique_ratio: float = 0.01) -> Dict[str, bool]:
        """
        Возвращает маску важных признаков на основе их вариативности и заполненности.

        :param df: DataFrame с данными для анализа
        :param threshold: порог важности для числовых признаков (стандартное отклонение)
        :param min_unique_ratio: минимальный процент уникальных значений для категориальных признаков
        :return: словарь {feature_name: is_important}
        """
        importance_mask = {}

        # Объединяем все колонки для анализа
        all_columns = self.numeric_cols + self.categorical_cols + self.date_cols

        for col in all_columns:
            try:
                if col in self.numeric_cols and col in df.columns:
                    # Для числовых признаков — проверяем стандартное отклонение
                    std_val = self._get_numeric_std(df, col)
                    if std_val is not None:
                        importance_mask[col] = std_val > threshold
                    else:
                        self.logger.debug(f"Не удалось рассчитать std для '{col}', признак помечен как неважный")
                        importance_mask[col] = False

                elif col in self.categorical_cols and col in df.columns:
                    # Для категориальных признаков — проверяем количество уникальных значений и их распределение
                    n_unique = self._get_categorical_unique_count(df, col)
                    total_count = len(df[col])

                    if n_unique is not None and total_count > 0:
                        unique_ratio = n_unique / total_count
                        # Признак важен, если:
                        # 1) больше 1 уникального значения
                        # 2) уникальные значения составляют достаточный процент от общего числа
                        importance_mask[col] = n_unique > 1 and unique_ratio >= min_unique_ratio
                    else:
                        importance_mask[col] = False

                elif col in self.date_cols and col in df.columns:
                    # Для дат считаем важными всегда (они несут временную информацию)
                    importance_mask[col] = True

                else:
                    # Если колонка отсутствует в DataFrame или не относится к известным типам
                    self.logger.warning(f"Колонка '{col}' отсутствует или не относится к известным типам, по умолчанию считается неважной")
                    importance_mask[col] = False

            except Exception as e:
                self.logger.error(f"Критическая ошибка при оценке важности признака '{col}': {e}")
                importance_mask[col] = False

        return importance_mask


    def _get_numeric_std(self, df: pd.DataFrame, col: str) -> Optional[float]:
        """Вспомогательный метод для расчёта стандартного отклонения числовой колонки."""
        try:
            if col in df.columns:
                # Учитываем только не-NaN значения
                numeric_data = df[col].dropna()
                if len(numeric_data) > 1:  # Нужно минимум 2 значения для расчёта std
                    return numeric_data.std()
            return None
        except Exception as e:
            self.logger.debug(f"Ошибка расчёта std для '{col}': {e}")
            return None

    def _get_categorical_unique_count(self, df: pd.DataFrame, col: str) -> Optional[int]:
        """Вспомогательный метод для подсчёта уникальных значений в категориальной колонке."""
        try:
            if col in df.columns:
                return df[col].nunique(dropna=True)  # Игнорируем NaN при подсчёте уникальных
            return None
        except Exception as e:
            self.logger.debug(f"Ошибка подсчёта уникальных значений для '{col}': {e}")
            return None
    
    def save_preprocessing_state(self, filepath: str) -> None:
        """
        Сохраняет состояние предобработки (энкодеры, скалеры) для последующего использования.

        :param filepath: путь для сохранения состояния
        """
        import pickle
        state = {
            'label_encoders': self.label_encoders,
            'scaler': self.scaler,
            'numeric_cols': self.numeric_cols,
            'categorical_cols': self.categorical_cols,
            'date_cols': self.date_cols,
            'target_col': self.target_col
        }
        with open(filepath, 'wb') as f:
            pickle.dump(state, f)
        self.logger.info(f"Состояние предобработки сохранено в {filepath}")

    def load_preprocessing_state(self, filepath: str) -> None:
        """
        Загружает состояние предобработки из файла.

        :param filepath: путь к файлу состояния
        """
        import pickle
        with open(filepath, 'rb') as f:
            state = pickle.load(f)
        self.label_encoders = state['label_encoders']
        self.scaler = state['scaler']
        self.numeric_cols = state['numeric_cols']
        self.categorical_cols = state['categorical_cols']
        self.date_cols = state['date_cols']
        self.target_col = state['target_col']
        self.logger.info(f"Состояние предобработки загружено из {filepath}")