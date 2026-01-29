import os
import re
import asyncio
import aiohttp
import shutil
import tempfile
import atexit
from datetime import date, datetime, timedelta, timezone
from pprint import pprint
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import urlopen
from typing import Optional, Tuple
from Logger import Logger
from SatPass import SatPas


class EusLogDownloader:
    """Клиент портала EUS для получения логов и графиков.

    Назначение:
        - Загружает HTML со списком станций и пролетов.
        - Парсит ссылки на логи и графики.
        - Скачивает лог-файлы и рендерит PNG-графики.
        - Формирует список SatPas для дальнейшей обработки.

    Методы:
        - __init__: инициализация клиента и параметров.
        - _validate_date_range: проверка корректности диапазона дат.
        - _build_date_params: формирование параметров t0/t1 для портала.
        - _extract_log_filename: имя лог-файла из URL или строки.
        - _normalize_view_url: построение полного URL log_view.
        - _register_child_process: регистрация дочернего процесса браузера.
        - _unregister_child_process: удаление процесса из отслеживания.
        - _cleanup_child_processes: завершение отслеживаемых процессов.
        - _load_html: загрузка HTML по URL и параметрам.
        - load_html_and_parse: возвращает BeautifulSoup для HTML.
        - get_station_list: возвращает список станций.
        - print_station_list: печатает список станций.
        - get_passes: возвращает список SatPas для станции.
        - print_passes: печатает список пролетов.
        - download_logs_file: скачивает лог-файлы.
        - download_graphs_file: рендерит PNG-графики.
    """
    # Инициализация
    def __init__(self, logger: Logger) -> None:
        """Создает клиент, подготавливает параметры и regex.

        Args:
            logger: Экземпляр Logger из Logger.py.

        Returns:
            None
        """
        if logger is None:
            raise ValueError("logger is required")
        self.logger = logger

        self.data_passes = {}
        self._child_processes = set()
        atexit.register(self._cleanup_child_processes)

        # Источники и параметры запроса.
        # http://eus.lorett.org/eus/logs_list.html - портал неоперативных станций
        # http://eus.lorett.org/eus/logs.html - портал оперативных станций
        self.urls = [
            "http://eus.lorett.org/eus/logs_list.html",
            "http://eus.lorett.org/eus/logs.html",
        ]

        # t0 - начальная дата, t1 - конечная дата (формат ГГГГ-ММ-ДД).
        # по умолчанию берется текущая дата и добавляется 1 день (нужно для портала EUS)
        start_dt = datetime.now(timezone.utc)
        end_dt = start_dt + timedelta(days=1)

        self.params: Tuple[datetime, datetime] = (start_dt, end_dt)
        # ширина и высота графика
        self.graph_viewport_width = 620
        self.graph_viewport_height = 680
        self.graph_load_delay = 0.5
        # скролл по x и y
        self.graph_scroll_x = 0
        self.graph_scroll_y = 0

        # Регулярные выражения для станций, строк таблицы, ячеек и ссылок на пролеты.
        # Ссылка на станцию: забираем значение stid.
        self.station_re = re.compile(r"logstation\.html\?stid=([^&\"']+)", re.I)

        # Строка таблицы с датой в формате YYYY-MM-DD и хвостом строки.
        self.date_row_re = re.compile(
            r"<tr>\s*<td[^>]*>\s*<b>\s*(\d{4}-\d{2}-\d{2})\s*</b>\s*</td>(.*?)</tr>",
            re.I | re.S,
        )

        # Содержимое ячеек <td> (включая многострочные).
        self.td_re = re.compile(r"<td[^>]*>(.*?)</td>", re.I | re.S)

        # Пара ссылок: log_view и log_get в пределах одной ячейки.
        self.pass_re = re.compile(
            r"href=['\"](log_view/[^'\"]+)['\"].*?"
            r"href=['\"](log_get/[^'\"]+)['\"]",
            re.I | re.S,
        )

        self.logger.info("EusLogPortal initialized")

    # Валидация диапазона дат
    def _validate_date_range(self, start_value: date, end_value: date) -> None:
        """Проверяет корректность диапазона дат (end > start).

        Args:
            start_value: Дата начала диапазона.
            end_value: Дата конца диапазона.

        Returns:
            None
        """
        self.logger.debug(f"validate dates: start={start_value}, end={end_value}")
        if end_value <= start_value:
            raise ValueError("end_day must be later than start_day")

    # Построение параметров дат
    def _build_date_params(
        self,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        ) -> dict:
        """Формирует параметры t0/t1 для запроса портала.

        Если задана только одна дата/время, конец автоматически = старт + 1 день.

        Args:
            start_dt: Дата/время начала (datetime).
            end_dt: Дата/время конца (datetime).

        Returns:
            dict: Параметры {"t0": "YYYY-MM-DD", "t1": "YYYY-MM-DD"}.
        """
        self.logger.debug(f"build date params: start_dt={start_dt}, end_dt={end_dt}")

        # если дата/время начала и конца не заданы, то используем текущую дату и добавляем 1 день
        if start_dt is None and end_dt is None:
            start_value = datetime.now(timezone.utc).date()
            end_value = start_value + timedelta(days=1)

        else:
            # если дата/время начала и конца заданы, то используем их
            start_value = start_dt.date() if start_dt is not None else None
            end_value = end_dt.date() if end_dt is not None else None

            # если дата/время начала не заданы, то используем дату конца
            if start_value is None and end_value is not None:
                start_value = end_value

            # если дата/время конца не заданы, то используем дату начала и добавляем 1 день
            if end_value is None and start_value is not None:
                end_value = start_value + timedelta(days=1)

        # проверяем корректность диапазона дат
        self._validate_date_range(start_value, end_value)

        # возвращаем параметры для запроса портала
        return {
            "t0": start_value.isoformat(),
            "t1": end_value.isoformat(),
        }

    # Скачивание одного файла лога (async)
    async def _download_single_log(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        url: str,
        out_dir: str,
        ) -> str:
        """Скачивает один лог-файл по URL, если еще не сохранен.

        Args:
            session: HTTP-сессия aiohttp.
            sem: Семафор для ограничения параллелизма.
            url: Прямая ссылка на log_get.
            out_dir: Каталог для сохранения.

        Returns:
            str: Путь к сохраненному файлу.
        """
        # создаем каталог для сохранения
        os.makedirs(out_dir, exist_ok=True)
        # извлекаем имя файла из URL
        # и создаем путь к файлу
        filename = os.path.basename(urlparse(url).path)
        path = os.path.join(out_dir, filename)
        # проверяем, если файл уже существует и не пустой, то пропускаем скачивание
        if os.path.exists(path) and os.path.getsize(path) > 0:
            self.logger.debug( f"file exists, skip: {path}")
            return path
        # скачиваем файл асинхронно     
        async with sem:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    async for chunk in r.content.iter_chunked(8192):
                        f.write(chunk)
        # сохраняем файл
        self.logger.debug( f"file saved: {path}")
        return path
    
    # Скачивание списка логов (async)
    async def _download_logs_async(self, tasks: list, max_parallel: int = 10) -> list:
        """Параллельно скачивает список логов и возвращает результаты.

        Args:
            tasks: Список (get_url, out_dir).
            max_parallel: Максимум одновременных скачиваний.

        Returns:
            list: Список путей или исключений.
        """
        # создаем семафор для ограничения параллелизма
        sem = asyncio.Semaphore(max_parallel)
        # создаем сессию для скачивания файлов
        async with aiohttp.ClientSession() as session:
            # создаем список задач для скачивания файлов
            download_tasks = []
            # добавляем задачи для скачивания файлов
            for get_url, out_dir in tasks:
                download_tasks.append(self._download_single_log(session, sem, get_url, out_dir))
            return await asyncio.gather(*download_tasks, return_exceptions=True)

    # Извлекает имя файла лога из URL просмотра или строки с именем файла.
    def _extract_log_filename(self, view_url_or_filename: str) -> str:
        """Извлекает имя файла лога из URL просмотра или возвращает строку.

        Args:
            view_url_or_filename: URL log_view или строка с именем файла.

        Returns:
            str: Имя файла лога.
        """
        if "log_view/" in view_url_or_filename or view_url_or_filename.startswith("http"):
            return os.path.basename(urlparse(view_url_or_filename).path)
        return view_url_or_filename

    # Строит полный URL просмотра из относительного пути или имени файла.
    def _normalize_view_url(self, view_url_or_filename: str) -> str:
        """Строит полный URL просмотра графика из относительной ссылки/имени.

        Args:
            view_url_or_filename: Относительная ссылка log_view или имя файла.

        Returns:
            str: Полный URL log_view.
        """
        # если URL начинается с http, то возвращаем его
        if view_url_or_filename.startswith("http"):
            return view_url_or_filename
        # если URL содержит log_view/, то возвращаем полный URL
        if "log_view/" in view_url_or_filename:
            # строим полный URL
            return urljoin("http://eus.lorett.org/eus/", view_url_or_filename)
        return urljoin("http://eus.lorett.org/eus/", f"log_view/{view_url_or_filename}")

    # Регистрирует дочерний процесс для последующей очистки.
    def _register_child_process(self, proc) -> None:
        """Регистрирует дочерний процесс для последующей очистки.

        Args:
            proc: Процесс браузера (subprocess-like).

        Returns:
            None
        """
        if proc is None:
            return
        self._child_processes.add(proc)

    # Удаляет дочерний процесс из списка на очистку.
    def _unregister_child_process(self, proc) -> None:
        """Удаляет процесс из списка отслеживания.

        Args:
            proc: Процесс браузера (subprocess-like).

        Returns:
            None
        """
        if proc is None:
            return
        self._child_processes.discard(proc)

    # Пытается завершить все отслеживаемые дочерние процессы.
    def _cleanup_child_processes(self) -> None:
        """Пытается корректно завершить все отслеживаемые процессы.

        Returns:
            None
        """
        for proc in list(self._child_processes):
            try:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except Exception:
                        proc.kill()
            except Exception:
                pass
            finally:
                self._child_processes.discard(proc)

    # Скачивает снимок графика пролета (async).
    async def _download_single_graph(
        self,
        sem: asyncio.Semaphore,
        view_url_or_filename: str,
        out_dir: str,
        ) -> str:
        """Рендерит страницу пролета и сохраняет PNG-график.

        Args:
            sem: Семафор для ограничения параллелизма.
            view_url_or_filename: URL log_view или имя файла лога.
            out_dir: Каталог для сохранения PNG.

        Returns:
            str: Путь к PNG или исключение.
        """
        # создаем каталог для сохранения
        os.makedirs(out_dir, exist_ok=True)
        # извлекаем имя файла лога из URL
        log_filename = self._extract_log_filename(view_url_or_filename)
        # если имя файла лога не найдено, то выбрасываем исключение
        if not log_filename:
            raise ValueError(f"invalid log filename: {view_url_or_filename}")

        # создаем имя файла для графика
        image_name = log_filename.replace(".log", ".png").replace(" ", "_")
        # создаем путь к файлу
        path = os.path.join(out_dir, image_name)
        # проверяем, если файл уже существует и не пустой, то пропускаем рендер
        if os.path.exists(path) and os.path.getsize(path) > 0:
            self.logger.debug(f"graph exists, skip: {path}")
            return path
        # строим полный URL для графика
        view_url = self._normalize_view_url(view_url_or_filename)
        self.logger.debug(f"graph download start: {view_url} -> {path}")
        # запускаем асинхронное скачивание графика
        async with sem:
            try:
                # используем playwright для рендера графика
                try:
                    # используем playwright для рендера графика
                    from playwright.async_api import async_playwright
                    # запускаем асинхронное скачивание графика
                    async with async_playwright() as p:
                        # запускаем браузер
                        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                        # создаем новую страницу
                        page = await browser.new_page()
                        # переходим на страницу
                        await page.goto(view_url, wait_until="networkidle", timeout=30000)
                        # устанавливаем размер viewport
                        await page.set_viewport_size(
                            {"width": self.graph_viewport_width, "height": self.graph_viewport_height}
                        )
                        # ждем загрузки графика
                        await asyncio.sleep(self.graph_load_delay)
                        # если скролл по x или y больше 0, то скроллим страницу
                        if self.graph_scroll_x > 0 or self.graph_scroll_y > 0:
                            await page.evaluate(f"window.scrollTo({self.graph_scroll_x}, {self.graph_scroll_y})")
                            await asyncio.sleep(0.2)
                        await page.screenshot(path=path, full_page=False)
                        # закрываем браузер
                        await browser.close()
                        # сохраняем график
                    self.logger.debug(f"graph saved: {path}")
                    return path
                except ImportError:
                    from pyppeteer import launch

                    os.environ["PYPPETEER_SKIP_CHROMIUM_DOWNLOAD"] = "1"
                    chrome_paths = [
                      r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                        os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe"),
                        r"C:\Program Files\Chromium\Application\chrome.exe",
                    ]
                    executable_path = None
                    # ищем путь к браузеру
                    for chrome_path in chrome_paths:
                        # если браузер найден, то устанавливаем путь
                        if os.path.exists(chrome_path):
                            executable_path = chrome_path
                            break
                    if not executable_path:
                        # если браузер не найден, то выбрасываем исключение
                        raise RuntimeError(
                            "Chrome/Chromium not found. Install Chrome or use: "
                            "pip install playwright && playwright install chromium"
                        )
                    user_data_dir = tempfile.mkdtemp(prefix="pyppeteer_user_data_")
                    # запускаем браузер
                    browser = await launch(
                        {
                            "executablePath": executable_path,
                            "userDataDir": user_data_dir,
                            "autoClose": False,
                            "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
                        }
                    )
                    self._register_child_process(browser.process)
                    # создаем новую страницу
                    page = await browser.newPage()
                    # переходим на страницу
                    await page.goto(view_url, waitUntil="networkidle0", timeout=30000)
                    # устанавливаем размер viewport
                    await page.setViewport(
                        {"width": self.graph_viewport_width, "height": self.graph_viewport_height}
                    )
                    # ждем загрузки графика
                    await asyncio.sleep(self.graph_load_delay)
                    # если скролл по x или y больше 0, то скроллим страницу
                    if self.graph_scroll_x > 0 or self.graph_scroll_y > 0:
                        await page.evaluate(f"window.scrollTo({self.graph_scroll_x}, {self.graph_scroll_y})")
                        await asyncio.sleep(0.2)
                    await page.screenshot({"path": path, "fullPage": False})
                    # закрываем браузер
                    try:
                        await browser.close()
                    except OSError as e:
                        # если закрывание браузера не удалось, то выводим предупреждение
                        self.logger.warning(f"pyppeteer close failed: {e}")
                    finally:
                        # удаляем процесс браузера
                        self._unregister_child_process(browser.process)
                        # удаляем каталог с пользовательскими данными
                        shutil.rmtree(user_data_dir, ignore_errors=True)
                    # сохраняем график
                    self.logger.info(f"graph saved: {path}")
                    return path
            except Exception as e:
                # если скачивание графика не удалось, то выводим исключение
                self.logger.exception(f"graph download failed: {view_url}", exc_info=e)
                return e

    # Скачивает несколько графиков параллельно (async).
    async def _download_graphs_async(self, tasks: list, max_parallel: int = 5) -> list:
        """Параллельно скачивает список графиков и возвращает результаты.

        Args:
            tasks: Список (view_url, out_dir).
            max_parallel: Максимум одновременных рендеров.

        Returns:
            list: Список путей или исключений.
        """
        # создаем семафор для ограничения параллелизма
        sem = asyncio.Semaphore(max_parallel)
        # создаем список задач для скачивания графиков
        download_tasks = []
        for view_url, out_dir in tasks:
            download_tasks.append(self._download_single_graph(sem, view_url, out_dir))
        return await asyncio.gather(*download_tasks, return_exceptions=True)

    # Получение текста страницы
    def _load_html(self, url: str, params: Optional[Tuple[datetime, datetime]] = None) -> str:
        """Получает HTML по URL с параметрами диапазона дат (если заданы).

        Args:
            url: Адрес страницы портала.
            params: Кортеж (start_dt, end_dt) или None.

        Returns:
            str: Текст HTML.
        """
        params = self.params if params is None else params
        if params is None:
            query = ""
        else:
            # если параметры не являются кортежем или не содержат 2 элемента, то выбрасываем исключение
            if not isinstance(params, tuple) or len(params) != 2:
                raise ValueError("params must be a tuple: (start_dt, end_dt)")
            start_dt, end_dt = params
            # если дата/время начала или конца не являются datetime объектами, то выбрасываем исключение
            if not isinstance(start_dt, datetime) or not isinstance(end_dt, datetime):
                raise TypeError("start_dt and end_dt must be datetime objects")
            query = urlencode(self._build_date_params(start_dt, end_dt))
        # если query не пустой, то добавляем его к URL
        if query:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{query}"
        # загружаем HTML по URL
        self.logger.debug( f"load url: {url}")
        with urlopen(url, timeout=3600) as r:
            text = r.read().decode("utf-8", errors="replace")
        # выводим информацию о загрузке
        self.logger.debug( f"load done: {url} bytes={len(text)}")
        self.logger.debug( f"html: {text}")

        return text

    # Загрузка и парсинг страницы
    def load_html_and_parse(
        self, params: Optional[Tuple[datetime, datetime]] = None
        ) -> dict:
        """Парсит страницы портала и возвращает станции с ссылками на пролеты.

        Args:
            params: Кортеж (start_dt, end_dt) или None.

        Returns:
            dict: Словарь {station: set((view_url, get_url))}.
        """
        passes = {}
        seen = {}
        for url in self.urls:
            # загружаем HTML по URL
            html = self._load_html(url, params=params)
            # Собираем станции в порядке на странице и ссылки на пролеты по станциям.
            self.logger.debug(f"parse page: base_url={url}, html_size={len(html)}")
            local = []
            for match in self.station_re.finditer(html):
                station = match.group(1)
                if station not in local:
                    local.append(station)
            # собираем станции в порядке на странице и ссылки на пролеты по станциям.
            for station in local:
                passes.setdefault(station, [])
                seen.setdefault(station, set())
            # собираем даты и ссылки на пролеты по станциям.
            for row in self.date_row_re.finditer(html):
                row_date = date.fromisoformat(row.group(1))
                cells = self.td_re.findall(row.group(2))
                for i, cell in enumerate(cells):
                    if i >= len(local):
                        break
                    station = local[i]
                    for p in self.pass_re.finditer(cell):
                        view_url = urljoin(url, p.group(1))
                        get_url = urljoin(url, p.group(2))
                        key = (view_url, get_url)
                        if key in seen[station]:
                            continue
                        seen[station].add(key)

                        # собираем ссылки на пролеты по станциям.
                        passes[station].append(
                            SatPas(
                                graph_url=view_url,
                                log_url=get_url,
                            )
                        )

        # собираем ссылки на пролеты по станциям.
        self.data_passes = passes
        return self.data_passes

    # Возвращает отсортированный список станций для текущих данных.
    def get_station_list(self) -> list:
        """Возвращает отсортированный список станций из data_passes.

        Returns:
            list: Список названий станций.
        """
        # собираем станции в порядке на странице и ссылки на пролеты по станциям.
        stations = sorted(list(self.data_passes.keys()))
        self.logger.info(f"stations {stations}")
        self.logger.debug( f"stations found: {len(stations)}")
        return stations

    # Печатает названия станций в stdout.
    def print_station_list(self) -> None:
        """Печатает список станций в stdout.

        Returns:
            None
        """
        stations = self.get_station_list()
        for station in stations:
            print(station)

    # Возвращает список пролетов для станции.
    def get_passes(self, station: str) -> list[SatPas]:
        """Возвращает список пролетов (SatPas) для станции.

        Args:
            station: Имя станции.

        Returns:
            list[SatPas]: Список пролетов для станции.
        """
        passes = self.data_passes
        if station in passes:
            # сортируем пролета по дате, URL лога и URL графика
            result = sorted(
                passes[station],
                key=lambda p: (p.pass_date or date.min, p.log_url or "", p.graph_url or ""),
            )
            self.logger.debug(f"passes exact match: station={station} passes={result}")
            return result
        # если пролета не найдены, то выводим информацию о не найденных пролетах
        self.logger.debug(f"passes not found: station={station}")
        return []

    # Печатает URL пролетов для станции.
    def print_passes(self, station: str) -> None:
        """Печатает список пролетов (view/get) в stdout.

        Args:
            station: Имя станции.

        Returns:
            None
        """ 
        # получаем список пролетов для станции
        passes = self.get_passes(station)
        # печатаем список пролетов
        for sat_pass in passes:
            print(f"{sat_pass.graph_url} {sat_pass.log_url}")

    # Скачивает файлы логов для указанных пролетов.
    def download_logs_file(self, passes_to_download: list, out_dir: str, max_parallel: int = 10) -> list:
        """Скачивает лог-файлы и раскладывает их по датам и станциям.

        Принимает список SatPas. Возвращает тот же список с заполненным log_path.

        Args:
            passes_to_download: Список SatPas.
            out_dir: Базовая директория для сохранения.
            max_parallel: Максимум одновременных скачиваний.

        Returns:
            list: Тот же список SatPas с заполненным log_path.
        """
        # создаем каталог для сохранения
        os.makedirs(out_dir, exist_ok=True)
        tasks = []
        date_re = re.compile(r"(\d{8})")
        # создаем регулярное выражение для извлечения названия станции
        station_re = re.compile(r"([^/\\\\]+?)__\d{8}")
        for index, item in enumerate(passes_to_download):
            if not isinstance(item, SatPas):
                raise ValueError("passes_to_download items must be SatPas")
            # если URL лога не найден, то выводим информацию о не найденном URL лога
            if not item.log_url:
                self.logger.warning("SatPas.log_url is empty, skip download")
                continue
            get_url = item.log_url
            station_name = item.station_name or "unknown_station"
            pass_date = item.pass_date  # дата пролета
            if pass_date:
                date_str = pass_date.strftime("%Y%m%d")
                # создаем путь к каталогу для сохранения
                date_dir = os.path.join(
                    out_dir, date_str[0:4], date_str[4:6], date_str[6:8], station_name
                )
            else: # если дата пролета не найдена, то создаем путь к каталогу для сохранения         
                date_match = date_re.search(get_url)
                station_match = station_re.search(get_url)
                station_name = station_match.group(1) if station_match else station_name
                if date_match: # если дата пролета найдена, то создаем путь к каталогу для сохранения
                    date_str = date_match.group(1)
                    date_dir = os.path.join(
                        out_dir, date_str[0:4], date_str[4:6], date_str[6:8], station_name
                    )
                else: # если дата пролета не найдена, то создаем путь к каталогу для сохранения
                    date_dir = os.path.join(out_dir, "unknown", "unknown", "unknown", station_name)
                    self.logger.warning(f"date not found in SatPas/url, using: {date_dir}")
            os.makedirs(date_dir, exist_ok=True) # создаем каталог для сохранения
            tasks.append((index, get_url, date_dir))

        if tasks:
            # запускаем асинхронное скачивание логов
            results = asyncio.run(
                self._download_logs_async([(url, dir_path) for _, url, dir_path in tasks], max_parallel=max_parallel)
            )
            for (index, _, _), result in zip(tasks, results): # скачиваем лог-файлы
                if isinstance(result, Exception):
                    self.logger.exception("download failed", exc_info=result)
                    passes_to_download[index].log_path = None # если скачивание лога не удалось, то устанавливаем log_path в None
                else:
                    passes_to_download[index].log_path = result # если скачивание лога удалось, то устанавливаем log_path в результат   
        return passes_to_download

    # Скачивает изображения графиков для указанных пролетов.
    def download_graphs_file(self, 
        passes_to_download: list,
        out_dir: str,
        max_parallel: int = 10
        ) -> list:

        """Скачивает PNG-графики и раскладывает их по датам и станциям.

        Принимает список SatPas. Возвращает тот же список с заполненным graph_path.

        Args:
            passes_to_download: Список SatPas.
            out_dir: Базовая директория для сохранения.
            max_parallel: Максимум одновременных рендеров.

        Returns:
            list: Тот же список SatPas с заполненным graph_path.
        """
        # создаем каталог для сохранения
        os.makedirs(out_dir, exist_ok=True)
        # создаем регулярное выражение для извлечения даты
        tasks = []
        date_re = re.compile(r"(\d{8})")
        # создаем регулярное выражение для извлечения названия станции
        station_re = re.compile(r"([^/\\\\]+?)__\d{8}")
        for index, item in enumerate(passes_to_download):
            if not isinstance(item, SatPas):
                raise ValueError("passes_to_download items must be SatPas")
            view_url = item.graph_url # URL графика
            if not view_url and item.log_url:
                view_url = self._normalize_view_url(self._extract_log_filename(item.log_url)) # строим полный URL графика
            if not view_url:
                self.logger.warning("SatPas.graph_url/log_url is empty, skip download")
                continue
            station_name = item.station_name or "unknown_station" # название станции
            pass_date = item.pass_date
            if pass_date: # если дата пролета найдена, то создаем путь к каталогу для сохранения
                date_str = pass_date.strftime("%Y%m%d")
                date_dir = os.path.join(
                    out_dir, date_str[0:4], date_str[4:6], date_str[6:8], station_name
                ) # создаем путь к каталогу для сохранения
            else: # если дата пролета не найдена, то создаем путь к каталогу для сохранения
                date_match = date_re.search(view_url)
                if not date_match: # если дата пролета не найдена, то извлекаем дату из URL графика         
                    log_filename = self._extract_log_filename(view_url)
                    date_match = date_re.search(log_filename)
                station_match = station_re.search(view_url) # извлекаем название станции из URL графика 
                if not station_match: # если название станции не найдено, то извлекаем название станции из URL лога
                    log_filename = self._extract_log_filename(view_url)
                    station_match = station_re.search(log_filename)
                station_name = station_match.group(1) if station_match else station_name # устанавливаем название станции
                if date_match: # если дата пролета найдена, то создаем путь к каталогу для сохранения
                    date_str = date_match.group(1)
                    date_dir = os.path.join(
                        out_dir, date_str[0:4], date_str[4:6], date_str[6:8], station_name
                    ) # создаем путь к каталогу для сохранения      
                else: # если дата пролета не найдена, то создаем путь к каталогу для сохранения
                    date_dir = os.path.join(out_dir, "unknown", "unknown", "unknown", station_name) # создаем путь к каталогу для сохранения
                    self.logger.warning(f"date not found in SatPas/url, using: {date_dir}") # выводим предупреждение
            os.makedirs(date_dir, exist_ok=True) # создаем каталог для сохранения
            tasks.append((index, view_url, date_dir))

        if tasks:
            # запускаем асинхронное скачивание графиков
            results = asyncio.run(
                self._download_graphs_async([(url, dir_path) for _, url, dir_path in tasks], max_parallel=max_parallel)
            )
            for (index, _, _), result in zip(tasks, results): # скачиваем графики
                if isinstance(result, Exception):
                    self.logger.exception("graph download failed", exc_info=result)
                    passes_to_download[index].graph_path = None # если скачивание графика не удалось, то устанавливаем graph_path в None
                else:
                    passes_to_download[index].graph_path = result # если скачивание графика удалось, то устанавливаем graph_path в результат    
        return passes_to_download


if __name__ == "__main__":
    from Logger import Logger

    # Логгер пишет в файл и консоль; уровень debug нужен для подробных трассировок.
    logger = Logger(path_log="eus_downloader", log_level="debug")

    # Инициализируем портал с логгером.
    portal = EusLogDownloader(logger=logger)

    # Диапазон дат: один день (end_day строго +1).
    start_dt = datetime(2026, 1, 25, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    params = (start_dt, end_dt)

    # Тест _load_html: получаем HTML и логируем размер.
    # html = portal._load_html(portal.urls[0], params=params)
    # portal.logger.info(f"load_html ok: bytes={len(html)}")

    # Тест load_and_parse: собираем станции и ссылки на пролеты.
    page_passes = portal.load_html_and_parse(params=params)
    portal.logger.info(f"stations in page: {len(page_passes)}")
    portal.logger.debug(f"page_passes: {page_passes}")

    # Тест get_station_list: сортированный список станций.
    station_list = portal.get_station_list()
    portal.logger.info(f"station_list ok: {len(station_list)}")

    # Тест get_passes: ссылки на пролеты для первой станции.
    if station_list:
        station = station_list[0]
        passes = portal.get_passes(station)
        portal.logger.info(f"passes for {station}: {len(passes)}")
        if passes:
            results = portal.download_logs_file(
                passes,
                out_dir="C:\\Users\\Yarik\\YandexDisk\\Engineering_local\\Soft\\GroundLinkMonitorServer\\passes_logs",
            )
            ok = sum(1 for r in results if r.log_path)
            fail = sum(1 for r in results if r.log_path is None)
            portal.logger.info(f"download_logs_file for {station}: ok={ok}, fail={fail}")
        else:
            portal.logger.warning(f"no passes for {station}")
    else:
        portal.logger.warning("no stations found")

    # Тест download_graphs_file:
    if station_list:
        if passes:
            results = portal.download_graphs_file(
                passes,
                out_dir="C:\\Users\\Yarik\\YandexDisk\\Engineering_local\\Soft\\GroundLinkMonitorServer\\passes_graphs",
            )
            ok = sum(1 for r in results if r.graph_path)
            fail = sum(1 for r in results if r.graph_path is None)
            portal.logger.info(f"download_graphs_file for {station}: ok={ok}, fail={fail}")

    portal.logger.debug(passes)


