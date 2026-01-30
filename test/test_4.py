#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
bulk_https_downloader.py

Скачивание большого количества текстовых логов по HTTPS быстро и экономно по памяти.

Использование:
  1) Подготовь файл urls.txt, где каждая строка — URL на .log (или любой текстовый файл)
  2) Запусти:
     python bulk_https_downloader.py --urls urls.txt --out logs --workers 80

Опции:
  --workers      количество параллельных воркеров (скорость vs нагрузка)
  --queue        размер очереди (не держим весь список задач в памяти)
  --chunk        размер чанка чтения (память ~ workers * chunk)
  --retries      число повторов на файл
  --timeout      общий таймаут (сек) на запрос
  --no-skip      не пропускать уже скачанные файлы
  --flat         складывать все файлы в одну папку (по умолчанию да)
"""

import asyncio
import argparse
import gc
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple
from urllib.parse import urlparse, unquote

import aiohttp


# ---------- Модель данных ----------

@dataclass(frozen=True)
class Item:
    url: str
    dst: Path


# ---------- Утилиты ----------

def safe_filename_from_url(url: str) -> str:
    """
    Берём имя файла из URL.
    Если URL заканчивается на / — генерим имя.
    """
    p = urlparse(url)
    name = Path(unquote(p.path)).name
    if not name:
        # fallback, чтобы не было пустого имени
        name = "downloaded.log"
    return name


def iter_items_from_urls_file(urls_path: str, out_dir: str, flat: bool = True) -> Iterable[Item]:
    """
    Лениво читает URL из файла (не грузит все 45k URL в память).
    """
    out = Path(out_dir)
    with open(urls_path, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if not url or url.startswith("#"):
                continue

            filename = safe_filename_from_url(url)

            # flat=True: logs/<filename>
            # flat=False: можно расширить под разбиение по датам/станциям
            dst = out / filename if flat else out / filename
            yield Item(url=url, dst=dst)


# ---------- Скачивание одного файла (явно закрываем response) ----------

async def fetch_to_file(
    session: aiohttp.ClientSession,
    item: Item,
    *,
    chunk_size: int,
    retries: int,
    skip_if_exists: bool,
) -> Tuple[bool, Optional[str]]:
    """
    Скачивает один файл потоково в *.part и затем атомарно переименовывает.

    Возвращает (downloaded, error_message_or_None)
    """
    url, dst = item.url, item.dst
    dst.parent.mkdir(parents=True, exist_ok=True)

    if skip_if_exists and dst.exists() and dst.stat().st_size > 0:
        return (False, None)

    tmp = dst.with_suffix(dst.suffix + ".part")

    last_err: Optional[BaseException] = None

    for attempt in range(retries + 1):
        resp: Optional[aiohttp.ClientResponse] = None
        try:
            resp = await session.get(url)
            resp.raise_for_status()

            # Важно: запись на диск идёт синхронно, но чанки небольшие,
            # RAM практически не растёт.
            with tmp.open("wb") as f:
                async for chunk in resp.content.iter_chunked(chunk_size):
                    if chunk:
                        f.write(chunk)

            tmp.replace(dst)
            return (True, None)

        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            last_err = e
            # backoff
            if attempt < retries:
                await asyncio.sleep(0.3 * (2 ** attempt))

        finally:
            # ЯВНО освобождаем response и возвращаем соединение в пул
            if resp is not None:
                try:
                    resp.release()
                except Exception:
                    pass
                try:
                    resp.close()
                except Exception:
                    pass

    # подчистим битый .part
    try:
        if tmp.exists():
            tmp.unlink()
    except OSError:
        pass

    return (False, f"{type(last_err).__name__}: {last_err}")


# ---------- Воркеры и пайплайн ----------

async def worker(
    wid: int,
    session: aiohttp.ClientSession,
    q: "asyncio.Queue[Optional[Item]]",
    *,
    chunk_size: int,
    retries: int,
    skip_if_exists: bool,
    stats: dict,
) -> None:
    """
    Воркер, который берёт Item из очереди и скачивает.
    """
    while True:
        item = await q.get()
        try:
            if item is None:
                return

            downloaded, err = await fetch_to_file(
                session, item,
                chunk_size=chunk_size,
                retries=retries,
                skip_if_exists=skip_if_exists,
            )

            if downloaded:
                stats["downloaded"] += 1
            else:
                if err is None:
                    stats["skipped"] += 1
                else:
                    stats["failed"] += 1
                    # Ошибки не копим в список (экономия RAM) — пишем строкой в файл.
                    if stats.get("errors_file"):
                        stats["errors_file"].write(f"{item.url}\t{item.dst}\t{err}\n")

        finally:
            q.task_done()


async def download_many(
    items: Iterable[Item],
    *,
    workers: int,
    queue_size: int,
    chunk_size: int,
    retries: int,
    skip_if_exists: bool,
    timeout_total: float,
    errors_log_path: Optional[str],
) -> dict:
    """
    Основной раннер. Делает явное закрытие session/connector и явную зачистку.
    Возвращает статистику.
    """
    timeout = aiohttp.ClientTimeout(
        total=timeout_total,
        connect=10,
        sock_connect=10,
        sock_read=timeout_total,
    )

    connector = aiohttp.TCPConnector(
        limit=workers * 2,
        limit_per_host=workers,
        ttl_dns_cache=300,
        keepalive_timeout=30,
        enable_cleanup_closed=True,
    )

    headers = {"User-Agent": "log-bulk-downloader/1.0"}

    q: asyncio.Queue[Optional[Item]] = asyncio.Queue(maxsize=queue_size)
    tasks = []
    session: Optional[aiohttp.ClientSession] = None

    stats = {"downloaded": 0, "skipped": 0, "failed": 0, "errors_file": None}

    try:
        # Ошибки пишем сразу в файл, не копим в памяти
        if errors_log_path:
            Path(errors_log_path).parent.mkdir(parents=True, exist_ok=True)
            stats["errors_file"] = open(errors_log_path, "a", encoding="utf-8")

        session = aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers)

        tasks = [
            asyncio.create_task(worker(
                i, session, q,
                chunk_size=chunk_size,
                retries=retries,
                skip_if_exists=skip_if_exists,
                stats=stats,
            ))
            for i in range(workers)
        ]

        # Кормим очередь постепенно (не создаём 45k задач)
        for it in items:
            await q.put(it)

        # Останавливаем воркеры
        for _ in range(workers):
            await q.put(None)

        await q.join()

        # Дождаться завершения воркеров
        await asyncio.gather(*tasks, return_exceptions=False)

        return {k: v for k, v in stats.items() if k != "errors_file"}

    finally:
        # Закрыть файл ошибок
        ef = stats.get("errors_file")
        if ef is not None:
            try:
                ef.flush()
                ef.close()
            except Exception:
                pass

        # Отменить подвисшие воркеры (на всякий случай)
        for t in tasks:
            if t and not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Закрыть session (закрывает коннекты)
        if session is not None:
            try:
                await session.close()
            except Exception:
                pass

        # Явно закрыть connector (обычно избыточно, но ты просил "закрыть всё")
        try:
            await connector.close()
        except Exception:
            pass

        # Убрать ссылки и форснуть GC
        try:
            del tasks
            del q
            del session
        except Exception:
            pass
        gc.collect()


# ---------- CLI ----------

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls", required=True, help="Путь к файлу со ссылками (по одной на строку)")
    ap.add_argument("--out", default="downloads", help="Папка для сохранения файлов")
    ap.add_argument("--workers", type=int, default=80, help="Параллельность (скорость vs нагрузка)")
    ap.add_argument("--queue", type=int, default=2000, help="Размер очереди (контроль RAM)")
    ap.add_argument("--chunk", type=int, default=64 * 1024, help="Размер чанка чтения (байт)")
    ap.add_argument("--retries", type=int, default=3, help="Число повторов на файл")
    ap.add_argument("--timeout", type=float, default=60.0, help="Таймаут (сек) на запрос")
    ap.add_argument("--no-skip", action="store_true", help="Не пропускать уже скачанные файлы")
    ap.add_argument("--errors", default="logs_download_errors.tsv", help="Файл для логирования ошибок (tsv). Поставь пусто чтобы отключить.")
    ap.add_argument("--flat", action="store_true", default=True, help="Складывать в одну папку (по умолчанию да)")
    return ap.parse_args()


def main():
    args = parse_args()

    errors_path = args.errors.strip() if args.errors else None
    items = iter_items_from_urls_file(args.urls, args.out, flat=args.flat)

    stats = asyncio.run(download_many(
        items,
        workers=args.workers,
        queue_size=args.queue,
        chunk_size=args.chunk,
        retries=args.retries,
        skip_if_exists=not args.no_skip,
        timeout_total=args.timeout,
        errors_log_path=errors_path,
    ))

    print("Done.")
    print(f"Downloaded: {stats['downloaded']}")
    print(f"Skipped:    {stats['skipped']}")
    print(f"Failed:     {stats['failed']}")
    if errors_path and stats["failed"]:
        print(f"Errors log: {errors_path}")


if __name__ == "__main__":
    main()
