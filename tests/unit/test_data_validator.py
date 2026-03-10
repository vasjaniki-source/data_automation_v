import pytest
import pandas as pd
from data_processor.data_validator import DataValidator
import numpy as np 

@pytest.fixture
def validator():
    return DataValidator()


def test_missing_and_duplicates_logic(validator):
    """Тест логики обработки пропущенных значений и дубликатов"""
    test_df = pd.DataFrame({
        'id': [1, 1, 2, 3],
        'val': [10.0, 10.0, 20.0, None]
    })

    is_valid, _ , cleaned_df = validator.validate(test_df)

    assert is_valid is False
    assert cleaned_df is not None
    assert len(cleaned_df) == 3  # После удаления дубликата
    assert cleaned_df['val'].isnull().sum() == 0  # Пропуски заполнены
    # Дополнительно проверяем, что медиана применена корректно
    assert cleaned_df.loc[cleaned_df['id'] == 3, 'val'].iloc[0] == 15.0  # Медиана = 15


def test_clean_data_status(validator):
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    
    is_valid, res, cleaned_df = validator.validate(df)
    
    assert res['total_missing'] == 0
    assert res['duplicates'] == 0
    assert res['validation_status'] == 'SUCCESS'
    assert is_valid 
    assert len(cleaned_df) == 2 
    assert cleaned_df.isnull().sum().sum() == 0