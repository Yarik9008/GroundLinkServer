#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Минимальный скрипт для анализа успешных пролетов станции.
Оптимизирован для максимальной производительности с использованием всех ядер CPU и потоков.
"""

import sys
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp


def detect_bend_type(log_file: Path) -> str:
    """Определяет тип диапазона (L или X) из заголовка файла."""
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if line.startswith('#Time'):
                    return "X" if 'Level2' in line else "L"
    except Exception:
        pass
    return "X"  # По умолчанию


def analyze_log_file_optimized(log_file_str: str, thresholds: dict) -> tuple:
    """Оптимизированный анализ одного лог-файла. Принимает строку пути для совместимости с multiprocessing."""
    log_file = Path(log_file_str)
    try:
        # Оптимизированное чтение: сначала определяем тип, затем читаем данные
        bend_type = "X"
        snr_index = 5
        threshold = thresholds["X"]
        
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            snr_values = []
            snr_sum = 0.0
            snr_count = 0
            
            for line in f:
                # Определяем тип диапазона из заголовка
                if line.startswith('#Time'):
                    if 'Level2' not in line:
                        bend_type = "L"
                        snr_index = 4
                        threshold = thresholds["L"]
                    continue
                
                # Пропускаем комментарии и пустые строки
                if not line.strip() or line.startswith('#'):
                    continue
                
                # Быстрый парсинг
                parts = line.split('\t')
                if len(parts) > snr_index:
                    try:
                        snr_val = float(parts[snr_index])
                        snr_sum += snr_val
                        snr_count += 1
                    except (ValueError, IndexError):
                        continue
        
        if snr_count > 0:
            avg_snr = snr_sum / snr_count
            return (avg_snr > threshold, bend_type, snr_count)
        return (False, bend_type, 0)
    except Exception:
        return (False, "X", 0)


def analyze_log_file(log_file: Path, thresholds: dict) -> tuple:
    """Обертка для совместимости со старым кодом."""
    result = analyze_log_file_optimized(str(log_file), thresholds)
    return (result[0], result[1])


def filter_log_files_by_date(log_files: list, start_date: str = None, end_date: str = None) -> list:
    """
    Фильтрует список файлов по дате из пути.
    
    Args:
        log_files: Список путей к файлам
        start_date: Начальная дата в формате YYYYMMDD (включительно)
        end_date: Конечная дата в формате YYYYMMDD (включительно)
    
    Returns:
        Отфильтрованный список файлов
    """
    if not start_date and not end_date:
        return log_files
    
    start_dt = None
    end_dt = None
    
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d")
        except ValueError:
            return log_files  # Если дата некорректна, возвращаем все файлы
    
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            return log_files
    
    filtered = []
    for file_path in log_files:
        # Путь: logs/YYYY/MM/YYYYMMDD/STATION/file.log
        # Извлекаем YYYYMMDD из пути
        path = Path(file_path)
        parts = path.parts
        
        # Ищем компонент, который выглядит как дата (8 цифр)
        file_date = None
        for part in parts:
            if part.isdigit() and len(part) == 8:
                try:
                    file_date = datetime.strptime(part, "%Y%m%d")
                    break
                except ValueError:
                    continue
        
        if file_date is None:
            continue  # Если не найдена дата, пропускаем файл
        
        # Проверяем диапазон дат
        if start_dt and file_date < start_dt:
            continue
        if end_dt and file_date > end_dt:
            continue
        
        filtered.append(file_path)
    
    return filtered


def find_available_stations(logs_dir: Path) -> list:
    """Находит все доступные станции в папке logs."""
    stations = set()
    
    # Ищем все папки со станциями (на один уровень глубже дат)
    for year_dir in logs_dir.iterdir():
        if year_dir.is_dir():
            for month_dir in year_dir.iterdir():
                if month_dir.is_dir():
                    for date_dir in month_dir.iterdir():
                        if date_dir.is_dir():
                            for station_dir in date_dir.iterdir():
                                if station_dir.is_dir() and any(station_dir.glob("*.log")):
                                    stations.add(station_dir.name)
    
    return sorted(list(stations))


def analyze_station_worker(args: tuple) -> dict:
    """Рабочая функция для анализа станции (используется в процессах)."""
    station_name, logs_dir_str, thresholds, max_thread_workers, start_date, end_date = args
    logs_dir = Path(logs_dir_str)
    
    # Ищем все .log файлы для станции
    log_files = [str(f) for f in logs_dir.rglob(f"{station_name}/*.log")]
    
    # Фильтруем по датам, если указаны
    if start_date or end_date:
        log_files = filter_log_files_by_date(log_files, start_date, end_date)
    
    if not log_files:
        return {"station": station_name, "total": 0, "successful": 0, "unsuccessful": 0}
    
    total = 0
    successful = 0
    
    # Используем ThreadPoolExecutor для параллельной обработки файлов внутри процесса
    with ThreadPoolExecutor(max_workers=max_thread_workers) as executor:
        # Запускаем анализ всех файлов параллельно
        future_to_file = {
            executor.submit(analyze_log_file_optimized, log_file, thresholds): log_file 
            for log_file in log_files
        }
        
        for future in as_completed(future_to_file):
            total += 1
            is_successful, _, _ = future.result()
            if is_successful:
                successful += 1
    
    unsuccessful = total - successful
    return {
        "station": station_name,
        "total": total,
        "successful": successful,
        "unsuccessful": unsuccessful
    }


def analyze_station(station_name: str, logs_dir: Path = Path("logs"), max_workers: int = None, 
                   use_multiprocessing: bool = False, measure_time: bool = True,
                   start_date: str = None, end_date: str = None):
    """Анализирует пролеты указанной станции с использованием параллельной обработки."""
    start_time = time.time() if measure_time else None
    
    # Пороги успешности
    thresholds = {"X": 7.0, "L": 0.0}
    
    # Ищем все .log файлы для станции
    log_files = list(logs_dir.rglob(f"{station_name}/*.log"))
    
    # Фильтруем по датам, если указаны
    if start_date or end_date:
        log_files = filter_log_files_by_date([str(f) for f in log_files], start_date, end_date)
        log_files = [Path(f) for f in log_files]
    
    if not log_files:
        print(f"Логи для станции '{station_name}' не найдены в {logs_dir}")
        return None
    
    print(f"\nАнализ станции: {station_name}")
    if start_date or end_date:
        date_range = f" с {start_date or 'начала'} по {end_date or 'конца'}"
        print(f"Период:{date_range}")
    print(f"Найдено файлов: {len(log_files)}")
    print("-" * 60)
    
    total = 0
    successful = 0
    
    # Определяем количество потоков для I/O операций
    if max_workers is None:
        # Оптимально для I/O: больше потоков, чем ядер CPU
        cpu_count = mp.cpu_count()
        max_workers = min(64, max(cpu_count * 4, len(log_files) // 10 + 8))
    
    analysis_start = time.time() if measure_time else None
    
    # Используем ThreadPoolExecutor для параллельной обработки файлов
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Запускаем анализ всех файлов параллельно
        future_to_file = {
            executor.submit(analyze_log_file_optimized, str(log_file), thresholds): log_file 
            for log_file in log_files
        }
        
        for future in as_completed(future_to_file):
            total += 1
            is_successful, _, _ = future.result()
            if is_successful:
                successful += 1
    
    analysis_time = time.time() - analysis_start if measure_time else None
    
    unsuccessful = total - successful
    success_rate = (successful / total * 100) if total > 0 else 0
    
    print(f"Всего пролетов: {total}")
    print(f"Успешных: {successful}")
    print(f"Неуспешных: {unsuccessful}")
    print(f"Процент успешности: {success_rate:.2f}%")
    
    if measure_time:
        total_time = time.time() - start_time
        print(f"Время анализа: {analysis_time:.2f} сек (общее время: {total_time:.2f} сек)")
        print(f"Производительность: {len(log_files)/analysis_time:.1f} файлов/сек")
    
    return {"total": total, "successful": successful, "unsuccessful": unsuccessful}


def analyze_all_stations(stations: list, logs_dir: Path, max_workers_stations: int = None, 
                        measure_time: bool = True, start_date: str = None, end_date: str = None):
    """Анализирует все станции параллельно с использованием всех ядер CPU и потоков."""
    start_time = time.time() if measure_time else None
    
    print("\n" + "=" * 60)
    print("АНАЛИЗ ВСЕХ СТАНЦИЙ (гибридная параллельная обработка)")
    print("=" * 60)
    
    if start_date or end_date:
        date_range = f" с {start_date or 'начала'} по {end_date or 'конца'}"
        print(f"Период:{date_range}")
    
    cpu_count = mp.cpu_count()
    print(f"Используется CPU ядер: {cpu_count}")
    
    # Пороги успешности
    thresholds = {"X": 7.0, "L": 0.0}
    
    # Оптимальное количество процессов: по количеству ядер CPU
    if max_workers_stations is None:
        max_workers_stations = min(len(stations), cpu_count)
    
    # Количество потоков на процесс: оптимально для I/O операций
    max_thread_workers = max(8, cpu_count * 2)
    
    print(f"Процессов: {max_workers_stations}, потоков на процесс: {max_thread_workers}")
    print("=" * 60)
    
    all_results = {}
    total_all = 0
    successful_all = 0
    
    # Подготавливаем аргументы для процессов
    logs_dir_str = str(logs_dir)
    process_args = [
        (station_name, logs_dir_str, thresholds, max_thread_workers, start_date, end_date)
        for station_name in stations
    ]
    
    analysis_start = time.time() if measure_time else None
    
    # Используем ProcessPoolExecutor для распределения по ядрам CPU
    # и ThreadPoolExecutor внутри каждого процесса для параллельной обработки файлов
    with ProcessPoolExecutor(max_workers=max_workers_stations) as executor:
        # Запускаем анализ всех станций параллельно
        future_to_station = {
            executor.submit(analyze_station_worker, args): args[0]
            for args in process_args
        }
        
        # Обрабатываем результаты по мере их готовности
        for future in as_completed(future_to_station):
            station_name = future_to_station[future]
            try:
                result = future.result()
                if result and result["total"] > 0:
                    all_results[station_name] = result
                    total_all += result["total"]
                    successful_all += result["successful"]
                    
                    # Выводим результат станции
                    success_rate = (result["successful"] / result["total"] * 100) if result["total"] > 0 else 0
                    print(f"\n{station_name}:")
                    print(f"  Всего: {result['total']}, Успешных: {result['successful']}, "
                          f"Неуспешных: {result['unsuccessful']}, Успешность: {success_rate:.2f}%")
            except Exception as e:
                print(f"\nОшибка при анализе станции {station_name}: {e}")
    
    analysis_time = time.time() - analysis_start if measure_time else None
    
    # Итоговая статистика
    unsuccessful_all = total_all - successful_all
    overall_success_rate = (successful_all / total_all * 100) if total_all > 0 else 0
    
    print("\n" + "=" * 60)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 60)
    print(f"Всего пролетов (все станции): {total_all}")
    print(f"Успешных: {successful_all}")
    print(f"Неуспешных: {unsuccessful_all}")
    print(f"Общий процент успешности: {overall_success_rate:.2f}%")
    
    if measure_time:
        total_time = time.time() - start_time
        print(f"\nВремя анализа: {analysis_time:.2f} сек (общее время: {total_time:.2f} сек)")
        print(f"Производительность: {total_all/analysis_time:.1f} файлов/сек")
    
    print("=" * 60)


def input_date(prompt: str, default: str = None) -> str:
    """Интерактивный ввод даты в формате YYYYMMDD."""
    while True:
        if default:
            user_input = input(f"{prompt} (по умолчанию: {default}, Enter для пропуска): ").strip()
        else:
            user_input = input(f"{prompt} (формат YYYYMMDD, Enter для пропуска): ").strip()
        
        if not user_input:
            return default
        
        # Проверяем формат даты
        if len(user_input) == 8 and user_input.isdigit():
            try:
                datetime.strptime(user_input, "%Y%m%d")
                return user_input
            except ValueError:
                print("Ошибка: некорректная дата. Используйте формат YYYYMMDD (например, 20251201)")
        else:
            print("Ошибка: дата должна состоять из 8 цифр (формат YYYYMMDD)")


def main():
    """Главная функция."""
    # Для Windows необходимо вызывать freeze_support() для multiprocessing
    if sys.platform == 'win32':
        mp.freeze_support()
    
    # Обработка аргументов командной строки
    if len(sys.argv) >= 3:
        # Формат: python simple_analyze.py <станция> <путь_к_логам>
        station_name = sys.argv[1]
        logs_dir = Path(sys.argv[2])
        start_date = None
        end_date = None
    elif len(sys.argv) >= 2:
        # Формат: python simple_analyze.py <станция> (используется logs по умолчанию)
        station_name = sys.argv[1]
        logs_dir = Path("logs")
        start_date = None
        end_date = None
    else:
        # Интерактивный режим
        station_name = None
        logs_dir = Path("logs")
        
        # Интерактивный ввод дат
        print("\n" + "=" * 60)
        print("НАСТРОЙКА ПЕРИОДА АНАЛИЗА")
        print("=" * 60)
        today = datetime.now().strftime("%Y%m%d")
        start_date = input_date("Дата начала анализа", None)
        end_date = input_date("Дата конца анализа", today)
        print("=" * 60)
    
    if not logs_dir.exists():
        print(f"Ошибка: папка {logs_dir} не найдена")
        sys.exit(1)
    
    # Если станция указана как аргумент
    if station_name:
        analyze_station(station_name, logs_dir, start_date=start_date, end_date=end_date)
        return
    
    # Иначе показываем список для выбора
    print("\nПоиск доступных станций...")
    stations = find_available_stations(logs_dir)
    
    if not stations:
        print(f"Станции не найдены в {logs_dir}")
        sys.exit(1)
    

    print(f"\nДоступные станции ({len(stations)}):")
    print("-" * 60)
    print(f"  0. Все станции")
    for i, station in enumerate(stations, 1):
        print(f"  {i}. {station}")
    print("-" * 60)
    
    while True:
        try:
            choice = input(f"\nВыберите станцию (0-{len(stations)}) или 'q' для выхода: ").strip()
            
            if choice.lower() == 'q':
                print("Выход...")
                sys.exit(0)
            
            choice_num = int(choice)
            
            if choice_num == 0:
                # Анализ всех станций
                analyze_all_stations(stations, logs_dir, start_date=start_date, end_date=end_date)
                break
            elif 1 <= choice_num <= len(stations):
                station_index = choice_num - 1
                station_name = stations[station_index]
                analyze_station(station_name, logs_dir, start_date=start_date, end_date=end_date)
                break
            else:
                print(f"Ошибка: введите число от 0 до {len(stations)}")
        except ValueError:
            print("Ошибка: введите число или 'q' для выхода")
        except KeyboardInterrupt:
            print("\n\nВыход...")
            sys.exit(0)


if __name__ == '__main__':
    main()
