import os
import pandas as pd
import pytest
from types import SimpleNamespace

from data_processor.pipeline import DataProcessingPipeline
from data_processor.data_loader import DataLoader

class DummyConfig:
    def __init__(self, data=None):
        self._data = data or {}
    def get(self, key, default=None):
        return self._data.get(key, default)
    def get_setting(self, key):
        return self._data.get(key)

@pytest.fixture
def tmp_csv(tmp_path):
    p = tmp_path / "sample.csv"
    p.write_text("a,b\n1,2\n3,4", encoding='utf-8')
    return str(p)

@pytest.fixture
def tmp_sql(tmp_path):
    p = tmp_path / "query.sql"
    p.write_text("SELECT 1 as a;", encoding='utf-8')
    return str(p)

def test_load_dataframe_direct():
    cfg = DummyConfig()
    pipeline = DataProcessingPipeline(cfg)
    df = pd.DataFrame({'x': [1,2,3]})
    res = pipeline.load_data(df)
    assert isinstance(res, pd.DataFrame)
    assert res.shape == (3,1)

def test_load_csv_type_path(tmp_csv):
    cfg = DummyConfig()
    pipeline = DataProcessingPipeline(cfg)
    df = pipeline.load_data(f"csv:{tmp_csv}")
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2
    assert list(df.columns) == ['a','b']

def test_load_nonexistent_no_fallback(tmp_path):
    cfg = DummyConfig()
    pipeline = DataProcessingPipeline(cfg)
    bad_path = tmp_path / "no_such_file.csv"
    res = pipeline.load_data(str(bad_path), allow_fallback_to_type=False)
    assert isinstance(res, pd.DataFrame)
    assert res.empty
    assert isinstance(pipeline.last_load_error, FileNotFoundError)

def test_load_bad_string_csv_slash():
    cfg = DummyConfig()
    pipeline = DataProcessingPipeline(cfg)
    res = pipeline.load_data("csv/excel")  # некорректный источник
    assert isinstance(res, pd.DataFrame)
    assert res.empty
    assert isinstance(pipeline.last_load_error, ValueError)

def test_sql_file_reads_and_calls_loader(tmp_sql, monkeypatch):
    # дадим config со строкой подключения, чтобы pipeline мог подставить conn_string
    cfg = DummyConfig({'db_connection_string': 'sqlite:///:memory:'})
    pipeline = DataProcessingPipeline(cfg)

    called = {}
    def fake_load(source, **kwargs):
        called['source'] = source
        called['kwargs'] = kwargs
        # возвращаем простую таблицу
        return pd.DataFrame({'a': [1]})
    monkeypatch.setattr(pipeline.loader, 'load', fake_load)

    df = pipeline.load_data(tmp_sql)
    assert isinstance(df, pd.DataFrame)
    assert called.get('source') == 'sql'
    assert 'sql_query' in called.get('kwargs', {})
    assert not df.empty