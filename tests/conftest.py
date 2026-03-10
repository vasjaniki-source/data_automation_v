import pytest
import pandas as pd
import os
import sys 

# Добавляем путь к корню проекта (data_automation_5)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Теперь импорты должны работать
from data_processor import DataProcessingPipeline
from utils.config_manager import ConfigManager

@pytest.fixture
def config_manager():
    """Фикстура для менеджера конфигурации."""
    return ConfigManager()

@pytest.fixture
def pipeline(config_manager):
    """Фикстура для основного пайплайна."""
    return DataProcessingPipeline(config_manager)

@pytest.fixture
def sample_data():
    """Базовый набор 'грязных' данных: дубликаты + пропуски."""
    return pd.DataFrame({
        'id': [1, 2, 2, 4],
        'name': ['Alice', 'Bob', 'Bob', None],
        'age': [25, 30, 30, 45],
        'salary': [50000, 60000, 60000, None]
    })

@pytest.fixture
def clean_data():
    """Чистые данные без проблем — для позитивных тестов."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'age': [25, 30, 35, 40],
        'salary': [50000, 60000, 70000, 80000]
    })

@pytest.fixture
def empty_data():
    """Пустой DataFrame — тест на обработку крайних случаев."""
    return pd.DataFrame()

@pytest.fixture
def data_all_missing():
    """Данные, где все значения пропущены."""
    return pd.DataFrame({
        'id': [None, None],
        'name': [None, None],
        'age': [None, None],
        'salary': [None, None]
    })

@pytest.fixture
def data_with_outliers():
    """Данные с выбросами в числовых столбцах."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 150],  # выброс: 150 лет
        'salary': [50000, 60000, 70000, 80000, 1000000]  # выброс: 1 млн
    })

@pytest.fixture
def data_different_dtypes():
    """Данные с разными типами столбцов для проверки конвертации."""
    return pd.DataFrame({
        'id': ['1', '2', '3', '4'],  # строки вместо чисел
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'age': ['25', '30', '35', '40'],  # строки
        'salary': [50000.5, 60000.8, 70000.2, 80000.1],  # float
        'join_date': ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01']  # даты как строки
    })

@pytest.fixture
def data_single_row():
    """Одна строка — минимальный кейс."""
    return pd.DataFrame({
        'id': [1],
        'name': ['Alice'],
        'age': [25],
        'salary': [50000]
    })