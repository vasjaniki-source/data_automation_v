import pandas as pd
import pytest
import tkinter as tk
from gui.gui_app import DataAutomationGUI
from unittest.mock import Mock, patch
import os

PIPELINE_CLASS_PATH = 'gui.gui_app.DataProcessingPipeline'

@pytest.fixture
def temp_csv_file(tmp_path):
    """
    Фикстура, создающая временный CSV файл для тестов.
    `tmp_path` fixture от pytest обеспечивает автоматическое удаление директории после теста.
    """
    csv_content = (
        "id,name,age,salary\n"
        "1,Alice,25,50000\n"
        "2,Bob,30,\n"
    )
    file_path = tmp_path / "test_data.csv"
    file_path.write_text(csv_content, encoding='utf-8')
    yield str(file_path)  # Возвращаем путь к файлу как строку


@pytest.fixture
def gui_app():
    """
    Фикстура для инициализации GUI приложения с замоканным пайплайном.
    Патчинг `DataProcessingPipeline` на уровне класса гарантирует, что любой экземпляр
    пайплайна, созданный в GUI (включая тот, что передаётся в FileSelectorWidget),
    будет нашим моком.
    """
    root = tk.Tk()

    # ПАТЧИНГ DataProcessingPipeline:
    # Перехватываем создание реального класса DataProcessingPipeline.
    with patch(PIPELINE_CLASS_PATH) as MockPipelineClass:
        # Создаём мок‑инстанс, который будет возвращён при вызове MockPipelineClass()
        mock_pipeline_instance = Mock()

        # Настраиваем возвращаемое значение для mock_pipeline_instance.load_data
        mock_loaded_df = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'salary': [50000.0, None]
        })
        mock_pipeline_instance.load_data.return_value = mock_loaded_df
        # Важно: current_df должен быть доступен и заполнен после «загрузки»
        mock_pipeline_instance.current_df = mock_loaded_df

        # Указываем, что MockPipelineClass() должен возвращать наш настроенный мок‑инстанс
        MockPipelineClass.return_value = mock_pipeline_instance

        # Теперь DataAutomationGUI при инициализации получит наш мок для self.pipeline
        app = DataAutomationGUI(root)

        # Дополнительная проверка, чтобы убедиться, что патчинг сработал:
        # Убеждаемся, что pipeline в GUI и внутри load_tab — это наш мок‑объект
        assert app.pipeline is mock_pipeline_instance
        assert app.load_tab.pipeline is mock_pipeline_instance

        # Обрабатываем отложенные задачи Tkinter, чтобы все виджеты были полностью созданы
        root.update_idletasks()

        # Сохраняем ссылку на мок‑объект пайплайна в `app` для удобства доступа в тестах
        app.mock_pipeline = mock_pipeline_instance

        yield app  # Возвращаем настроенное приложение GUI для выполнения теста
    root.destroy()  # Уничтожаем Tkinter root после завершения теста для очистки ресурсов

def test_gui_initial_state(gui_app):
    """
    Проверка начального состояния GUI:
    Убеждаемся, что дефолтное значение source_var на вкладке загрузки установлено правильно.
    """
    assert gui_app.load_tab.source_var.get() == "CSV/Excel"

@patch('gui.widgets.file_selector.filedialog.askopenfilename')  # Правильный путь к filedialog
def test_gui_load_button_logic(mock_askopenfilename,  gui_app, temp_csv_file):
    mock_pipeline = gui_app.pipeline

    # 1. Настраиваем GUI для режима CSV/Excel
    gui_app.load_tab.source_var.set("CSV/Excel")
    gui_app.load_tab.toggle_inputs()

    # 2. Настраиваем мок: возвращаем путь к файлу
    mock_askopenfilename.return_value = temp_csv_file


    # 3. Имитируем выбор файла
    gui_app.load_tab.browse_file()

    # Диагностика: что попало в path_ent?
    actual_path = gui_app.load_tab.path_ent.get()
    print(f"Ожидаемый путь: {temp_csv_file}")
    print(f"Фактический путь в Entry: {actual_path}")
    print(f"Тип значения в Entry: {type(actual_path)}")
    print(f"mock_askopenfilename.return_value: {mock_askopenfilename.return_value}")
    print(f"Вызовы askopenfilename: {mock_askopenfilename.call_args_list}")

    # Проверки после выбора файла
    mock_askopenfilename.assert_called_once()


    assert actual_path == temp_csv_file, (
        f"Ожидаемый путь '{temp_csv_file}', фактический '{actual_path}'"
    )

    # 4. Имитируем загрузку данных
    gui_app.load_tab.load_data()

    # 5. Проверяем вызов load_data с правильными аргументами
    mock_pipeline.load_data.assert_called_once_with(
        'csv/excel',
        file_path=temp_csv_file
    )
    # 7. Проверяем обновление current_df в пайплайне
    assert mock_pipeline.current_df is not None
    assert len(mock_pipeline.current_df) == 2

    # 8. Проверяем обновление info_label
    expected_info_text = "Загружено: 2 строк, 4 колонок"
    actual_info_text = gui_app.load_tab.info_label.cget("text")
    assert expected_info_text in actual_info_text, (
        f"Ожидался текст '{expected_info_text}', получен '{actual_info_text}'"
    )