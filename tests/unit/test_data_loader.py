from unittest.mock import MagicMock, patch
import pytest
import pandas as pd
from data_processor.data_loader import DataLoader
from utils.config_manager import ConfigManager

@pytest.fixture
def mock_config():
    return ConfigManager()

@pytest.fixture
def loader(mock_config):
    return DataLoader(mock_config)

def test_check_size_valid(loader, tmp_path):
    """Проверка валидации размера файла."""
    test_file = tmp_path / "small.csv"
    test_file.write_text("col1,col2\n1,2")
    # Не должно вызывать исключение
    loader._check_size(str(test_file))

def test_unsupported_source(loader):
    """Проверка реакции на неизвестный источник."""
    with pytest.raises(ValueError, match="не поддерживается"):
        loader.load(source="unknown_source")

def test_loader_invalid_source(config_manager):
    """Проверка реакции на неподдерживаемый источник."""
    loader = DataLoader(config_manager)
    with pytest.raises(ValueError, match="не поддерживается"):
        loader.load(source="unknown")

@patch("requests.get")
def test_load_api_json_success(mock_get, config_manager):
    """Тест эмуляции успешной загрузки JSON через API."""
    loader = DataLoader(config_manager)
    
    # Настройка мока (имитация ответа сервера)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {'Content-Type': 'application/json'}
    mock_response.json.return_value = [{"col1": "val1"}, {"col1": "val2"}]
    mock_get.return_value = mock_response

    df = loader.load(source="api", api_url="http://fake-api.com")
    assert not df.empty
    assert len(df) == 2