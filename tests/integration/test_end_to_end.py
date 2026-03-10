import pytest
import os
import pandas as pd
from data_processor.pipeline import DataProcessingPipeline
from utils.config_manager import ConfigManager

def test_full_pipeline_flow(tmp_path):
    """
    Сквозной тест (End-to-End):
    Проверка полного цикла обработки данных от загрузки до генерации отчета.
    """
    
    # 1. Инициализация компонентов
    # Используем временную директорию tmp_path (предоставляется pytest) для конфигов и данных
    config_file = tmp_path / "test_config.json"
    config_manager = ConfigManager(config_file=str(config_file))
    pipeline = DataProcessingPipeline(config_manager)

    # 2. Создание "грязного" тестового набора данных
    # Строка 1 и 2 - дубликаты. Строка 3 - содержит пропуск (NaN)
    csv_content = (
        "id,name,age,salary\n"
        "1,Alice,25,50000\n"
        "1,Alice,25,50000\n"
        "2,Bob,30,\n"  # Пропущен salary
        "3,Charlie,35,70000\n"
    )
    data_file = tmp_path / "input_data.csv"
    data_file.write_text(csv_content, encoding='utf-8')

    # 3. ЗАГРУЗКА ДАННЫХ
    # Передаем na_values, чтобы пустые строки точно стали NaN
    df_loaded = pipeline.load_data(
        source_type='csv', 
        file_path=str(data_file), 
        na_values=["", " ", "NaN"]
    )
    
    assert df_loaded is not None
    assert len(df_loaded) == 4
    assert pipeline.current_df is not None

    # 4. ВАЛИДАЦИЯ
    # Pipeline.run_validation вызывает Validator.validate и возвращает отчет (dict)
    validation_report = pipeline.run_validation()
    
    assert isinstance(validation_report, dict)
    # Проверяем, что валидатор нашел проблемы до очистки
    assert validation_report.get('total_missing', 0) >= 1, "Должен быть найден минимум 1 пропуск"
    assert validation_report.get('duplicates', 0) >= 1, "Должен быть найден минимум 1 дубликат"
    assert validation_report.get('validation_status') == 'WARNING'

    # 5. ОЧИСТКА

    pipeline.run_cleaning()
    
    # После очистки: 4 строки - 1 дубликат = 3 строки
    assert len(pipeline.current_df) == 3, f"Ожидалось 3 строки после очистки, получено {len(pipeline.current_df)}"
    # Проверяем, что пропусков больше нет
    assert pipeline.current_df.isnull().sum().sum() == 0, "После очистки не должно быть пропусков"

    # 6. АНАЛИЗ
    # Запуск статистического анализа
    analysis_results = pipeline.run_full_analysis()
    assert analysis_results is not None
    assert 'statistics' in analysis_results or 'summary' in str(analysis_results).lower()

 # 7. ГЕНЕРАЦИЯ ОТЧЕТА
    
    report_path = pipeline.generate_report(output_format="pdf") 
    
    assert report_path is not None
    assert os.path.exists(report_path), f"Файл отчета не найден по пути: {report_path}"
    assert report_path.endswith(".pdf"), f"Файл отчета должен иметь расширение .pdf, но получен {report_path}"


def test_pipeline_error_handling(pipeline):
    """Тест обработки ошибки отсутствия данных"""
    with pytest.raises(
        ValueError,
        match="Нет данных для анализа. Сначала загрузите данные."
    ):
        pipeline.run_full_analysis()
