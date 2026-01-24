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
from Logger import Logger


class EusLogPortal:
    """Клиент портала EUS.

    Загружает HTML со списком станций/пролетов, парсит ссылки,
    скачивает лог-файлы и строит PNG-графики через браузерный рендер.
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
        today = datetime.now(timezone.utc).date()
        self.params = {
            "t0": today.isoformat(),
            "t1": (today + timedelta(days=1)).isoformat(),
        }
        self.graph_viewport_width = 620
        self.graph_viewport_height = 680
        self.graph_load_delay = 0.5
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
    def _build_date_params(self, start_day=None, end_day=None):
        """Формирует параметры t0/t1 для запроса портала.

        Если задана только одна дата, конец автоматически = старт + 1 день.

        Args:
            start_day: Дата начала (date или ISO-строка).
            end_day: Дата конца (date или ISO-строка).

        Returns:
            dict: Параметры {"t0": "YYYY-MM-DD", "t1": "YYYY-MM-DD"}.
        """
        self.logger.debug(f"build date params: start_day={start_day}, end_day={end_day}")
        if start_day is None and end_day is None:
            start_value = datetime.now(timezone.utc).date()
            end_value = start_value + timedelta(days=1)
        else:
            if start_day is not None:
                start_value = start_day if isinstance(start_day, date) else date.fromisoformat(start_day)
            else:
                start_value = None
            if end_day is not None:
                end_value = end_day if isinstance(end_day, date) else date.fromisoformat(end_day)
            else:
                end_value = None

            if start_value is None and end_value is not None:
                start_value = end_value
            if end_value is None and start_value is not None:
                end_value = start_value + timedelta(days=1)

        self._validate_date_range(start_value, end_value)

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
        os.makedirs(out_dir, exist_ok=True)

        filename = os.path.basename(urlparse(url).path)
        path = os.path.join(out_dir, filename)

        if os.path.exists(path) and os.path.getsize(path) > 0:
            self.logger.debug( f"file exists, skip: {path}")
            return path

        async with sem:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    async for chunk in r.content.iter_chunked(8192):
                        f.write(chunk)

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
        sem = asyncio.Semaphore(max_parallel)
        async with aiohttp.ClientSession() as session:
            download_tasks = []
            for get_url, out_dir in tasks:
                download_tasks.append(self._download_single_log(session, sem, get_url, out_dir))
            return await asyncio.gather(*download_tasks, return_exceptions=True)

    # Извлекает имя файла лога из URL просмотра или строки с именем файла.
    def _extract_log_filename(self, view_url_or_filename: str) -> str:
        """Извлекает имя файла лога из URL просмотра или возвращает строку."""
        if "log_view/" in view_url_or_filename or view_url_or_filename.startswith("http"):
            return os.path.basename(urlparse(view_url_or_filename).path)
        return view_url_or_filename

    # Строит полный URL просмотра из относительного пути или имени файла.
    def _normalize_view_url(self, view_url_or_filename: str) -> str:
        """Строит полный URL просмотра графика из относительной ссылки/имени."""
        if view_url_or_filename.startswith("http"):
            return view_url_or_filename
        if "log_view/" in view_url_or_filename:
            return urljoin("http://eus.lorett.org/eus/", view_url_or_filename)
        return urljoin("http://eus.lorett.org/eus/", f"log_view/{view_url_or_filename}")

    # Регистрирует дочерний процесс для последующей очистки.
    def _register_child_process(self, proc) -> None:
        """Регистрирует дочерний процесс для последующей очистки."""
        if proc is None:
            return
        self._child_processes.add(proc)

    # Удаляет дочерний процесс из списка на очистку.
    def _unregister_child_process(self, proc) -> None:
        """Удаляет процесс из списка отслеживания."""
        if proc is None:
            return
        self._child_processes.discard(proc)

    # Пытается завершить все отслеживаемые дочерние процессы.
    def _cleanup_child_processes(self) -> None:
        """Пытается корректно завершить все отслеживаемые процессы."""
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
        os.makedirs(out_dir, exist_ok=True)
        log_filename = self._extract_log_filename(view_url_or_filename)
        if not log_filename:
            raise ValueError(f"invalid log filename: {view_url_or_filename}")

        image_name = log_filename.replace(".log", ".png").replace(" ", "_")
        path = os.path.join(out_dir, image_name)

        if os.path.exists(path) and os.path.getsize(path) > 0:
            self.logger.debug(f"graph exists, skip: {path}")
            return path

        view_url = self._normalize_view_url(view_url_or_filename)
        self.logger.debug(f"graph download start: {view_url} -> {path}")

        async with sem:
            try:
                try:
                    from playwright.async_api import async_playwright

                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                        page = await browser.new_page()
                        await page.goto(view_url, wait_until="networkidle", timeout=30000)
                        await page.set_viewport_size(
                            {"width": self.graph_viewport_width, "height": self.graph_viewport_height}
                        )
                        await asyncio.sleep(self.graph_load_delay)
                        if self.graph_scroll_x > 0 or self.graph_scroll_y > 0:
                            await page.evaluate(f"window.scrollTo({self.graph_scroll_x}, {self.graph_scroll_y})")
                            await asyncio.sleep(0.2)
                        await page.screenshot(path=path, full_page=False)
                        await browser.close()
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
                    for chrome_path in chrome_paths:
                        if os.path.exists(chrome_path):
                            executable_path = chrome_path
                            break
                    if not executable_path:
                        raise RuntimeError(
                            "Chrome/Chromium not found. Install Chrome or use: "
                            "pip install playwright && playwright install chromium"
                        )
                    user_data_dir = tempfile.mkdtemp(prefix="pyppeteer_user_data_")
                    browser = await launch(
                        {
                            "executablePath": executable_path,
                            "userDataDir": user_data_dir,
                            "autoClose": False,
                            "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
                        }
                    )
                    self._register_child_process(browser.process)
                    page = await browser.newPage()
                    await page.goto(view_url, waitUntil="networkidle0", timeout=30000)
                    await page.setViewport(
                        {"width": self.graph_viewport_width, "height": self.graph_viewport_height}
                    )
                    await asyncio.sleep(self.graph_load_delay)
                    if self.graph_scroll_x > 0 or self.graph_scroll_y > 0:
                        await page.evaluate(f"window.scrollTo({self.graph_scroll_x}, {self.graph_scroll_y})")
                        await asyncio.sleep(0.2)
                    await page.screenshot({"path": path, "fullPage": False})
                    try:
                        await browser.close()
                    except OSError as e:
                        self.logger.warning(f"pyppeteer close failed: {e}")
                    finally:
                        self._unregister_child_process(browser.process)
                        shutil.rmtree(user_data_dir, ignore_errors=True)
                    self.logger.info(f"graph saved: {path}")
                    return path
            except Exception as e:
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
        sem = asyncio.Semaphore(max_parallel)
        download_tasks = []
        for view_url, out_dir in tasks:
            download_tasks.append(self._download_single_graph(sem, view_url, out_dir))
        return await asyncio.gather(*download_tasks, return_exceptions=True)

    # Получение текста страницы
    def load_html(self, url: str, params=None) -> str:
        """Получает HTML по URL с параметрами диапазона дат (если заданы).

        Args:
            url: Адрес страницы портала.
            params: Параметры запроса или None.

        Returns:
            str: Текст HTML.
        """
        params = self.params if params is None else params
        query = urlencode(params) if params else ""
        if query:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{query}"

        self.logger.debug( f"load url: {url}")
        with urlopen(url, timeout=60) as r:
            text = r.read().decode("utf-8", errors="replace")
        self.logger.debug( f"load done: {url} bytes={len(text)}")
        self.logger.debug( f"html: {text}")

        return text

    # Загрузка и парсинг страницы
    def load_html_and_parse(self, params=None) -> dict:
        """Парсит страницы портала и возвращает станции с ссылками на пролеты.

        Args:
            params: Параметры запроса t0/t1 или None.

        Returns:
            dict: Словарь {station: set((view_url, get_url))}.
        """
        passes = {}
        for url in self.urls:
            html = self.load_html(url, params=params)
            # Собираем станции в порядке на странице и ссылки на пролеты по станциям.
            self.logger.debug(f"parse page: base_url={url}, html_size={len(html)}")
            local = []
            for match in self.station_re.finditer(html):
                station = match.group(1)
                if station not in local:
                    local.append(station)

            for station in local:
                passes.setdefault(station, set())

            for row in self.date_row_re.finditer(html):
                cells = self.td_re.findall(row.group(2))
                for i, cell in enumerate(cells):
                    if i >= len(local):
                        break
                    station = local[i]
                    for p in self.pass_re.finditer(cell):
                        passes[station].add((
                            urljoin(url, p.group(1)),
                            urljoin(url, p.group(2)),
                        ))

        self.data_passes = passes
        return self.data_passes


    # Возвращает отсортированный список станций для текущих данных.
    def get_station_list(self) -> list:
        """Возвращает отсортированный список станций из data_passes.

        Returns:
            list: Список названий станций.
        """
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
    def get_passes(self, station: str) -> list:
        """Возвращает список пролетов (view/get) для станции.

        Args:
            station: Имя станции.

        Returns:
            list: Список (view_url, get_url).
        """
        passes = self.data_passes
        if station in passes:
            result = sorted(passes[station])
            self.logger.debug( f"passes exact match: station={station} passes={result}")
            return result

        self.logger.debug( f"passes not found: station={station}")
        return []


    # Печатает URL пролетов для станции.
    def print_passes(self, station: str) -> None:
        """Печатает список пролетов (view/get) в stdout.

        Args:
            station: Имя станции.

        Returns:
            None
        """
        passes = self.get_passes(station)
        for view_url, get_url in passes:
            print(f"{view_url} {get_url}")


    # Скачивает файлы логов для указанных пролетов.
    def download_logs_file(self, passes_to_download: list, out_dir: str = "C:\\Users\\Yarik\\YandexDisk\\Engineering_local\\Soft\\GroundLinkMonitorServer\\passes_logs", max_parallel: int = 10) -> list:
        """Скачивает лог-файлы и раскладывает их по датам и станциям.

        Принимает список пар (view_url, get_url) или (get_url, out_dir).

        Args:
            passes_to_download: Список пар ссылок.
            out_dir: Базовая директория для сохранения.
            max_parallel: Максимум одновременных скачиваний.

        Returns:
            list: Пути к файлам или исключения.
        """
        os.makedirs(out_dir, exist_ok=True)
        tasks = []
        date_re = re.compile(r"(\d{8})")
        station_re = re.compile(r"([^/\\\\]+?)__\d{8}")
        for item in passes_to_download:
            if len(item) == 2:
                view_or_get, second = item
                if second.startswith("http"):
                    get_url = second
                    date_match = date_re.search(get_url)
                    station_match = station_re.search(get_url)
                    station_name = station_match.group(1) if station_match else "unknown_station"
                    if date_match:
                        date_str = date_match.group(1)
                        date_dir = os.path.join(out_dir, date_str[0:4], date_str[4:6], date_str[6:8], station_name)
                        os.makedirs(date_dir, exist_ok=True)
                        tasks.append((get_url, date_dir))
                    else:
                        date_dir = os.path.join(out_dir, "unknown", "unknown", "unknown", station_name)
                        os.makedirs(date_dir, exist_ok=True)
                        self.logger.warning(f"date not found in url, using: {date_dir}")
                        tasks.append((get_url, date_dir))
                else:
                    get_url = view_or_get
                    date_match = date_re.search(get_url)
                    station_match = station_re.search(get_url)
                    station_name = station_match.group(1) if station_match else "unknown_station"
                    if date_match:
                        date_str = date_match.group(1)
                        date_dir = os.path.join(out_dir, date_str[0:4], date_str[4:6], date_str[6:8], station_name)
                        os.makedirs(date_dir, exist_ok=True)
                        tasks.append((get_url, date_dir))
                    else:
                        date_dir = os.path.join(out_dir, "unknown", "unknown", "unknown", station_name)
                        os.makedirs(date_dir, exist_ok=True)
                        self.logger.warning(f"date not found in url, using: {date_dir}")
                        tasks.append((get_url, date_dir))
            else:
                raise ValueError("passes_to_download items must be (view_url, get_url) or (get_url, out_dir)")

        results = asyncio.run(self._download_logs_async(tasks, max_parallel=max_parallel))
        for result in results:
            if isinstance(result, Exception):
                self.logger.exception("download failed", exc_info=result)
        return results

    # Скачивает изображения графиков для указанных пролетов.
    def download_graphs_file(self, passes_to_download: list, out_dir: str = "C:\\Users\\Yarik\\YandexDisk\\Engineering_local\\Soft\\GroundLinkMonitorServer\\passes_graphs", max_parallel: int = 10) -> list:
        """Скачивает PNG-графики и раскладывает их по датам и станциям.

        Принимает список пар (view_url, get_url) или (view_url, out_dir).

        Args:
            passes_to_download: Список пар ссылок.
            out_dir: Базовая директория для сохранения.
            max_parallel: Максимум одновременных рендеров.

        Returns:
            list: Пути к файлам или исключения.
        """
        os.makedirs(out_dir, exist_ok=True)
        tasks = []
        date_re = re.compile(r"(\d{8})")
        station_re = re.compile(r"([^/\\\\]+?)__\d{8}")
        for item in passes_to_download:
            if len(item) == 2:
                view_url, second = item
                target_dir = out_dir
                if not second.startswith("http"):
                    target_dir = second

                date_match = date_re.search(view_url)
                if not date_match:
                    log_filename = self._extract_log_filename(view_url)
                    date_match = date_re.search(log_filename)

                station_match = station_re.search(view_url)
                if not station_match:
                    log_filename = self._extract_log_filename(view_url)
                    station_match = station_re.search(log_filename)

                station_name = station_match.group(1) if station_match else "unknown_station"
                if date_match:
                    date_str = date_match.group(1)
                    date_dir = os.path.join(
                        target_dir, date_str[0:4], date_str[4:6], date_str[6:8], station_name
                    )
                    os.makedirs(date_dir, exist_ok=True)
                    tasks.append((view_url, date_dir))
                else:
                    date_dir = os.path.join(target_dir, "unknown", "unknown", "unknown", station_name)
                    os.makedirs(date_dir, exist_ok=True)
                    self.logger.warning(f"date not found in url, using: {date_dir}")
                    tasks.append((view_url, date_dir))
            else:
                raise ValueError("passes_to_download items must be (view_url, get_url) or (view_url, out_dir)")

        results = asyncio.run(self._download_graphs_async(tasks, max_parallel=max_parallel))
        for result in results:
            if isinstance(result, Exception):
                self.logger.exception("graph download failed", exc_info=result)
        return results



if __name__ == "__main__":
    from Logger import Logger

    # Логгер пишет в файл и консоль; уровень debug нужен для подробных трассировок.
    logger = Logger(path_log="eus_downloader", log_level="debug")

    # Инициализируем портал с логгером.
    portal = EusLogPortal(logger=logger)

    # Диапазон дат: один день (end_day строго +1).
    start_day = datetime.now(timezone.utc).date()
    end_day = start_day + timedelta(days=1)
    params = portal._build_date_params("2026-01-23", "2026-01-24")

    # Тест load_html: получаем HTML и логируем размер.
    html = portal.load_html(portal.urls[0], params=params)
    portal.logger.info(f"load_html ok: bytes={len(html)}")

    # Тест load_and_parse: собираем станции и ссылки на пролеты.
    page_passes = portal.load_html_and_parse(params=params)
    portal.logger.info(f"stations in page: {len(page_passes)}")
    portal.logger.debug(f"page_passes: {page_passes}")

    # Тест get_station_list: сортированный список станций.
    station_list = portal.get_station_list()
    portal.logger.info(f"station_list ok: {len(station_list)}")

    # Тест get_passes + download_logs_file:
    # Берем первую станцию и скачиваем только один лог,
    # чтобы не нагружать портал лишними запросами.
    if station_list:
        station = station_list[0]
        passes = portal.get_passes(station)
        portal.logger.info(f"passes for {station}: {len(passes)}")
        if passes:
            results = portal.download_logs_file(
                [passes[0]],
                out_dir="C:\\Users\\Yarik\\YandexDisk\\Engineering_local\\Soft\\GroundLinkMonitorServer\\passes_logs",
            )
            ok = sum(1 for r in results if isinstance(r, str))
            fail = sum(1 for r in results if isinstance(r, Exception))
            portal.logger.info(f"download_logs_file for {station}: ok={ok}, fail={fail}")
        else:
            portal.logger.warning(f"no passes for {station}")
    else:
        portal.logger.warning("no stations found")

    # Тест download_graphs_file:
    # Рендерим один график по первому пролету,
    # чтобы проверить работу Playwright/Pyppeteer.
    if station_list:
        station = station_list[0]
        passes = portal.get_passes(station)
        if passes:
            results = portal.download_graphs_file(
                [passes[0]],
                out_dir="C:\\Users\\Yarik\\YandexDisk\\Engineering_local\\Soft\\GroundLinkMonitorServer\\passes_graphs",
            )
            ok = sum(1 for r in results if isinstance(r, str))
            fail = sum(1 for r in results if isinstance(r, Exception))
            portal.logger.info(f"download_graphs_file for {station}: ok={ok}, fail={fail}")

