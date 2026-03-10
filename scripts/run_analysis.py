import argparse
import sys
import os

# Добавление корня проекта в путь поиска
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processor import DataProcessingPipeline
from utils.config_manager import ConfigManager

def main():
    parser = argparse.ArgumentParser(description="CLI интерфейс системы автоматизации данных.")
    parser.add_argument("--source", type=str, required=True, help="Тип: csv, api, sql")
    parser.add_argument("--path", type=str, help="Путь к файлу или URL API")
    parser.add_argument("--report", action="store_true", help="Сгенерировать PDF отчет")

    args = parser.parse_args()
    
    config = ConfigManager()
    pipeline = DataProcessingPipeline(config)

    print(f"[*] Запуск обработки для источника: {args.source}")
    
    try:
        # Загрузка
        params = {'file_path': args.path} if args.source == 'csv' else {'api_url': args.path}
        pipeline.load_data(args.source, **params)
        
        # Полный цикл
        pipeline.run_validation()
        pipeline.run_cleaning()
        pipeline.run_full_analysis()
        
        if args.report:
            path = pipeline.generate_report("pdf")
            print(f"[+] Отчет создан: {path}")
            
        print("[+] Обработка завершена успешно.")
    except Exception as e:
        print(f"[!] Ошибка: {e}")

if __name__ == "__main__":
    main()