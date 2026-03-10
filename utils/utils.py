import os
import json
import yaml
from typing import Dict, Any, Union
import pandas as pd


class GeneralUtils:
    """Объединённый модуль общих утилит"""

    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """Загрузка конфигурации из файла (JSON/YAML)"""
        ext = os.path.splitext(config_path)[1].lower()
        with open(config_path, 'r', encoding='utf-8') as f:
            if ext in ['.json', '.js']:
                return json.load(f)
            elif ext in ['.yaml', '.yml']:
                return yaml.safe_load(f)
            else:
                raise ValueError(f"Неподдерживаемый формат файла: {ext}")

    @staticmethod
    def save_config(config: Dict[str, Any], config_path: str):
        """Сохранение конфигурации в файл"""
        ext = os.path.splitext(config_path)[1].lower()
        with open(config_path, 'w', encoding='utf-8') as f:
            if ext in ['.json', '.js']:
                json.dump(config, f, indent=2, ensure_ascii=False)
            elif ext in ['.yaml', '.yml']:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
            else:
                raise ValueError(f"Неподдерживаемый формат файла: {ext}")

    @staticmethod
    def create_directory(path: str) -> bool:
        """Создание директории, если её не существует"""
        try:
            os.makedirs(path, exist_ok=True)
            return True
        except Exception as e:
            print(f"Ошибка создания директории {path}: {e}")
            return False

    @staticmethod
    def safe_divide(a: float, b: float) -> float:
        """Безопасное деление с обработкой деления на ноль"""
        try:
            return a / b
        except ZeroDivisionError:
            return 0.0

    @staticmethod
    def clean_dataframe(df: pd.DataFrame, numeric_threshold: float = 0.8) -> pd.DataFrame:
        """
        Очистка DataFrame и приведение типов с порогом допустимого числа нечисловых значений.

        Parameters:
        -----------
        df : pd.DataFrame
            Исходный DataFrame.
        numeric_threshold : float, default 0.8
            Минимальная доля значений, которые должны успешно преобразоваться в число,
            чтобы столбец был приведён к числовому типу.
        """
        df_cleaned = df.copy()

        for col in df_cleaned.columns:
            if df_cleaned[col].dtype == 'object':
                # Убираем пробелы
                df_cleaned[col] = df_cleaned[col].astype(str).str.strip()

                # Пробуем преобразовать в числовой тип
                temp_numeric = pd.to_numeric(df_cleaned[col], errors='coerce')
                non_null_ratio = temp_numeric.notna().sum() / len(temp_numeric)

                # Если достаточно значений преобразовалось — заменяем
                if non_null_ratio >= numeric_threshold:
                    df_cleaned[col] = temp_numeric

        return df_cleaned
    @staticmethod
    def validate_file_exists(file_path: str) -> bool:
        """Проверка существования файла"""
        return os.path.isfile(file_path)

    @staticmethod
    def get_file_size(file_path: str) -> int:
        """Получение размера файла в байтах"""
        if GeneralUtils.validate_file_exists(file_path):
            return os.path.getsize(file_path)
        return 0