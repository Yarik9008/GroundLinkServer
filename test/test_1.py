import asyncio
import os
import re
from pprint import pprint
from urllib.parse import urljoin, urlparse

import aiohttp

URLS = [
    "http://eus.lorett.org/eus/logs_list.html",
    "http://eus.lorett.org/eus/logs.html",
]

PARAMS = {"t0": "2026-01-01", "t1": "2026-01-11"}

STATION_RE = re.compile(r"logstation\.html\?stid=([^&\"']+)", re.I)
DATE_ROW_RE = re.compile(
    r"<tr>\s*<td[^>]*>\s*<b>\s*(\d{4}-\d{2}-\d{2})\s*</b>\s*</td>(.*?)</tr>",
    re.I | re.S,
)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.I | re.S)
PASS_RE = re.compile(
    r"href=['\"](log_view/[^'\"]+)['\"].*?"
    r"href=['\"](log_get/[^'\"]+)['\"]",
    re.I | re.S,
)


# -------- Парсинг HTML в структуру station -> set((view, get)) --------

def parse_page(html: str, base_url: str, stations: list, passes: dict):
    # станции на этой странице (локальный порядок колонок)
    local = []
    for m in STATION_RE.finditer(html):
        st = m.group(1)
        if st not in local:
            local.append(st)

    # добавляем в общий порядок и словарь
    for st in local:
        if st not in stations:
            stations.append(st)
        passes.setdefault(st, set())

    # строки по датам -> ячейки -> пролёты
    for row in DATE_ROW_RE.finditer(html):
        cells = TD_RE.findall(row.group(2))
        for i, cell in enumerate(cells):
            if i >= len(local):
                break
            st = local[i]
            for p in PASS_RE.finditer(cell):
                passes[st].add((
                    urljoin(base_url, p.group(1)),  # log_view
                    urljoin(base_url, p.group(2)),  # log_get
                ))


# -------- Асинхронная загрузка --------

async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url, params=PARAMS, timeout=aiohttp.ClientTimeout(total=60)) as r:
        r.raise_for_status()
        return await r.text()


async def download_file(session: aiohttp.ClientSession, sem: asyncio.Semaphore, url: str, out_dir: str) -> str:
    """
    Скачивает log_get URL в out_dir. Возвращает путь к файлу.
    """
    os.makedirs(out_dir, exist_ok=True)

    filename = os.path.basename(urlparse(url).path)
    path = os.path.join(out_dir, filename)

    # если уже скачан — пропускаем
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path

    async with sem:  # ограничиваем параллельность
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                async for chunk in r.content.iter_chunked(8192):
                    f.write(chunk)

    return path


async def main():
    stations = []
    passes = {}  # station -> set((view, get))

    sem = asyncio.Semaphore(10)  # сколько файлов качать одновременно

    async with aiohttp.ClientSession(headers={"User-Agent": "EUS-async-downloader/1.0"}) as session:
        # 1) скачать и распарсить обе страницы
        for src in URLS:
            try:
                html = await fetch_text(session, src)
                parse_page(html, src, stations, passes)
            except Exception as e:
                print(f"[WARN] source {src} skipped: {e}")

        # 2) собрать задачи на скачивание всех log_get
        tasks = []
        for st in stations:
            out_dir = os.path.join("logs", st)
            for _view, get_url in passes.get(st, set()):
                tasks.append(download_file(session, sem, get_url, out_dir))

        # 3) скачать всё параллельно (ошибки не роняют весь запуск)
        results = await asyncio.gather(*tasks, return_exceptions=True)

    ok = sum(1 for r in results if isinstance(r, str))
    fail = sum(1 for r in results if isinstance(r, Exception))
    print(f"[INFO] downloads: ok={ok}, fail={fail}")

    # 4) вывести итоговую структуру как ты просил
    out = [
        [st, [list(pair) for pair in sorted(passes.get(st, set()))]]
        for st in stations
    ]
    pprint(out)


if __name__ == "__main__":
    asyncio.run(main())
