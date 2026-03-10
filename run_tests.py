import pytest
import sys
import os

def main():
    """Запуск автоматизированного тестирования проекта."""
    print("=== Запуск автоматических тестов проекта data_automation_5 ===")
    
    # Аргументы: 
    # -v (подробно), 
    # -s (вывод принтов), 
    # --tb=short (короткие трейсбеки)
    exit_code = pytest.main(["tests", "-v", "--tb=short"])
    
    if exit_code == 0:
        print("\n[ВCE ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО]")
    else:
        print(f"\n[ОШИБКА: Некоторые тесты провалились. Код выхода: {exit_code}]")
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()