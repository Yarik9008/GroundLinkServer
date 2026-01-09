#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер для страницы https://eus.lorett.org/eus/loglist_frames.html
Извлекает информацию о логах и фреймах из HTML страницы
"""

import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional, Tuple, Union
from urllib.parse import urljoin
import urllib3

# Отключаем предупреждения о небезопасных SSL соединениях
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class LogListFramesParser:
    """Парсер для страницы loglist_frames.html"""
    
    def __init__(self, base_url: str = "https://eus.lorett.org", timeout: int = 30):
        """
        Инициализация парсера
        
        Args:
            base_url: Базовый URL сайта
            timeout: Таймаут для HTTP запросов в секундах
        """
        self.base_url = base_url
        self.timeout = timeout
        self.url = urljoin(base_url, "/eus/loglist_frames.html")
        
    def fetch_page(self, raise_on_error: bool = True) -> Tuple[str, int]:
        """
        Загружает HTML страницу
        
        Args:
            raise_on_error: Если True, выбрасывает исключение при ошибке HTTP
            
        Returns:
            Tuple[str, int]: (HTML содержимое страницы, HTTP статус код)
            
        Raises:
            requests.RequestException: При ошибках HTTP запроса (если raise_on_error=True)
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        r = requests.get(self.url, headers=headers, timeout=self.timeout, verify=False)
        if raise_on_error:
            r.raise_for_status()
        return r.text, r.status_code
    
    def parse(self, html: Optional[Union[str, Tuple[str, int]]] = None) -> Dict:
        """
        Парсит HTML страницу и извлекает информацию о логах/фреймах
        
        Args:
            html: HTML содержимое или кортеж (html, status_code) (если None, загружается с сервера)
            
        Returns:
            Dict: Словарь с распарсенными данными:
                - 'frames': список фреймов/логов
                - 'links': список всех ссылок
                - 'dates': найденные даты
                - 'log_files': список лог-файлов
                - 'metadata': метаданные страницы
                - 'tables': список таблиц (если есть)
        """
        status_code = None
        if html is None:
            html, status_code = self.fetch_page(raise_on_error=False)
        elif isinstance(html, tuple):
            html, status_code = html
        
        # Убеждаемся, что html - это строка
        if not isinstance(html, str):
            raise ValueError(f"html должен быть строкой, получен: {type(html)}")
        
        soup = BeautifulSoup(html, 'html.parser')
        result = {
            'frames': [],
            'links': [],
            'dates': set(),
            'log_files': [],
            'metadata': {
                'status_code': status_code
            }
        }
        
        # Извлекаем заголовок страницы
        if soup.title and soup.title.string:
            result['metadata']['title'] = soup.title.string.strip()
        
        # Ищем все ссылки на логи
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            link_text = link.get_text(strip=True)
            
            # Сохраняем все ссылки
            full_url = urljoin(self.url, href)
            result['links'].append({
                'href': href,
                'text': link_text,
                'full_url': full_url
            })
            
            # Ищем логи (файлы с расширением .log)
            if '.log' in href.lower():
                log_info = self._parse_log_link(href, link_text)
                if log_info:
                    result['log_files'].append(log_info)
                    # Извлекаем дату из имени файла
                    date_match = re.search(r'(\d{8})', href)
                    if date_match:
                        result['dates'].add(date_match.group(1))
            
            # Ищем фреймы (iframe или ссылки содержащие "frame")
            if 'frame' in href.lower() or 'frame' in link_text.lower():
                result['frames'].append({
                    'href': href,
                    'text': link_text,
                    'full_url': full_url
                })
        
        # Ищем iframe элементы
        for iframe in soup.find_all('iframe'):
            src = iframe.get('src', '')
            if src:
                full_url = urljoin(self.url, src)
                result['frames'].append({
                    'href': src,
                    'text': iframe.get('name', ''),
                    'full_url': full_url,
                    'type': 'iframe'
                })
        
        # Преобразуем dates в список для JSON сериализации
        result['dates'] = sorted(list(result['dates']))
        
        # Извлекаем таблицы, если есть
        tables = soup.find_all('table')
        if tables:
            result['tables'] = []
            for table in tables:
                table_data = self._parse_table(table)
                if table_data:
                    result['tables'].append(table_data)
        
        return result
    
    def _parse_log_link(self, href: str, text: str) -> Optional[Dict]:
        """
        Парсит ссылку на лог-файл
        
        Args:
            href: URL ссылки
            text: Текст ссылки
            
        Returns:
            Dict: Информация о логе или None
        """
        # Паттерн для логов: имя_станции__дата_время_rec.log
        log_pattern = r'([^/]+)__(\d{8})[^/]*_rec\.log'
        match = re.search(log_pattern, href)
        
        if match:
            station_name = match.group(1)
            date = match.group(2)
            
            return {
                'station_name': station_name,
                'date': date,
                'filename': href.split('/')[-1] if '/' in href else href,
                'href': href,
                'text': text,
                'full_url': urljoin(self.url, href)
            }
        
        # Альтернативный паттерн для других форматов логов
        if '.log' in href:
            return {
                'filename': href.split('/')[-1] if '/' in href else href,
                'href': href,
                'text': text,
                'full_url': urljoin(self.url, href)
            }
        
        return None
    
    def _parse_table(self, table) -> Optional[Dict]:
        """
        Парсит HTML таблицу
        
        Args:
            table: BeautifulSoup объект таблицы
            
        Returns:
            Dict: Данные таблицы
        """
        rows = []
        headers = []
        
        # Ищем заголовки
        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        
        # Ищем строки данных
        tbody = table.find('tbody') or table
        for tr in tbody.find_all('tr'):
            cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if cells:
                rows.append(cells)
        
        if rows or headers:
            return {
                'headers': headers,
                'rows': rows,
                'row_count': len(rows)
            }
        
        return None
    
    def get_logs_by_date(self, date: str, html: Optional[str] = None) -> List[Dict]:
        """
        Получает все логи для указанной даты
        
        Args:
            date: Дата в формате YYYYMMDD
            html: HTML содержимое (если None, загружается с сервера)
            
        Returns:
            List[Dict]: Список логов для указанной даты
        """
        parsed = self.parse(html)
        return [log for log in parsed['log_files'] if log.get('date') == date]
    
    def get_logs_by_station(self, station_name: str, html: Optional[str] = None) -> List[Dict]:
        """
        Получает все логи для указанной станции
        
        Args:
            station_name: Имя станции
            html: HTML содержимое (если None, загружается с сервера)
            
        Returns:
            List[Dict]: Список логов для указанной станции
        """
        parsed = self.parse(html)
        return [log for log in parsed['log_files'] 
                if log.get('station_name', '').lower() == station_name.lower()]


def main():
    """Пример использования парсера"""
    parser = LogListFramesParser()
    
    try:
        print(f"Загрузка страницы: {parser.url}")
        # Пробуем загрузить, но не выбрасываем исключение при ошибке HTTP
        # чтобы можно было увидеть структуру страницы ошибки
        html, status_code = parser.fetch_page(raise_on_error=False)
        
        if status_code != 200:
            print(f"⚠️  Внимание: сервер вернул статус {status_code}")
            if status_code == 503:
                print("   Сервер временно недоступен. Парсер все равно обработает HTML.")
            print()
        
        result = parser.parse(html=(html, status_code))
        
        print(f"=== Результаты парсинга ===")
        print(f"Заголовок: {result['metadata'].get('title', 'N/A')}")
        print(f"\nНайдено дат: {len(result['dates'])}")
        if result['dates']:
            print(f"Даты: {', '.join(result['dates'][:10])}{'...' if len(result['dates']) > 10 else ''}")
        
        print(f"\nНайдено лог-файлов: {len(result['log_files'])}")
        if result['log_files']:
            print("\nПервые 5 логов:")
            for log in result['log_files'][:5]:
                print(f"  - {log.get('filename', 'N/A')}")
                if 'station_name' in log:
                    print(f"    Станция: {log['station_name']}, Дата: {log.get('date', 'N/A')}")
        
        print(f"\nНайдено фреймов: {len(result['frames'])}")
        if result['frames']:
            print("\nПервые 5 фреймов:")
            for frame in result['frames'][:5]:
                print(f"  - {frame.get('text', 'N/A')}: {frame.get('href', 'N/A')}")
        
        print(f"\nНайдено ссылок: {len(result['links'])}")
        if result['links']:
            print("\nПервые 5 ссылок:")
            for link in result['links'][:5]:
                print(f"  - {link.get('text', 'N/A')}: {link.get('href', 'N/A')}")
        
        if 'tables' in result and result['tables']:
            print(f"\nНайдено таблиц: {len(result['tables'])}")
            for i, table in enumerate(result['tables'], 1):
                print(f"  Таблица {i}: {table['row_count']} строк, {len(table['headers'])} колонок")
        
        print(f"\n✅ Парсер успешно обработал страницу!")
        print(f"\nПример использования в коде:")
        print(f"  parser = LogListFramesParser()")
        print(f"  result = parser.parse()  # Загрузит и распарсит страницу")
        print(f"  logs = parser.get_logs_by_date('20240101')  # Получить логи по дате")
        print(f"  logs = parser.get_logs_by_station('STATION_NAME')  # Получить логи по станции")
        
    except requests.RequestException as e:
        print(f"❌ Ошибка при загрузке страницы: {e}")
    except Exception as e:
        print(f"❌ Ошибка при парсинге: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
