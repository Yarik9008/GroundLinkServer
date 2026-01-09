#!/usr/bin/env python3
"""
Тест обработки 503 ошибки в LorettGroundLinkMonitor
"""

import sys
import time
from unittest.mock import Mock, patch
import requests

# Импортируем функцию для тестирования
sys.path.insert(0, '/root/lorett/GroundLinkMonitorServer')
from LorettGroundLinkMonitor import fetch_logs_page

def test_503_retry():
    """Тестирует retry логику при 503 ошибке"""
    
    print("=" * 80)
    print("ТЕСТ ОБРАБОТКИ 503 ОШИБКИ")
    print("=" * 80)
    
    # Создаем мок-ответ с 503 ошибкой
    mock_response = Mock()
    mock_response.status_code = 503
    mock_response.raise_for_status.side_effect = requests.HTTPError(response=mock_response)
    
    # Создаем успешный ответ для последней попытки
    success_response = Mock()
    success_response.status_code = 200
    success_response.text = "<html>Success</html>"
    success_response.raise_for_status.return_value = None
    
    print("\n1️⃣  Тест: Сервер недоступен (503), затем восстанавливается")
    print("-" * 80)
    
    with patch('requests.get') as mock_get:
        # Первая попытка - 503, вторая попытка - 503, третья - успех
        mock_get.side_effect = [
            mock_response,  # Попытка 1: 503
            mock_response,  # Попытка 2: 503
            success_response  # Попытка 3: Успех
        ]
        
        start_time = time.time()
        try:
            result = fetch_logs_page("http://test.com", "test", {}, max_retries=3)
            elapsed = time.time() - start_time
            print(f"✓ Успешно получен ответ после повторных попыток")
            print(f"✓ Время выполнения: {elapsed:.2f} сек (ожидается ~6 сек из-за задержек 2+4)")
            print(f"✓ Результат: {result}")
        except Exception as e:
            print(f"✗ ОШИБКА: {e}")
    
    print("\n2️⃣  Тест: Сервер постоянно недоступен (все попытки 503)")
    print("-" * 80)
    
    with patch('requests.get') as mock_get:
        # Все попытки возвращают 503
        mock_get.return_value = mock_response
        
        start_time = time.time()
        try:
            result = fetch_logs_page("http://test.com", "test", {}, max_retries=3)
            print(f"✗ ОШИБКА: Не выброшено исключение!")
        except requests.HTTPError as e:
            elapsed = time.time() - start_time
            print(f"✓ Корректно выброшено HTTPError после всех попыток")
            print(f"✓ Время выполнения: {elapsed:.2f} сек (ожидается ~14 сек: 2+4+8)")
            print(f"✓ Тип ошибки: {type(e).__name__}")
    
    print("\n3️⃣  Тест: Другая HTTP ошибка (404) - без повторов")
    print("-" * 80)
    
    mock_404 = Mock()
    mock_404.status_code = 404
    mock_404.raise_for_status.side_effect = requests.HTTPError(response=mock_404)
    
    with patch('requests.get') as mock_get:
        mock_get.return_value = mock_404
        
        start_time = time.time()
        try:
            result = fetch_logs_page("http://test.com", "test", {}, max_retries=3)
            print(f"✗ ОШИБКА: Не выброшено исключение!")
        except requests.HTTPError as e:
            elapsed = time.time() - start_time
            print(f"✓ Корректно выброшено HTTPError без повторов")
            print(f"✓ Время выполнения: {elapsed:.2f} сек (должно быть < 1 сек)")
            print(f"✓ Код ошибки: {e.response.status_code if e.response else 'unknown'}")
    
    print("\n" + "=" * 80)
    print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
    print("=" * 80)

if __name__ == "__main__":
    test_503_retry()
