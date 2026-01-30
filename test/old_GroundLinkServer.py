import requests
from bs4 import BeautifulSoup
import json
import re
import urllib3
import sys
import shutil
import os
import logging
import smtplib
from pathlib import Path
from urllib.parse import urljoin, quote
from collections import defaultdict
from colorama import init, Fore, Style
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Dict, Any
import asyncio
import time
import sqlite3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage

BASE_DIR = Path("/root/lorett/GroundLinkServer")

# --- test_1.py-style HTML parsing (stations as columns + direct log_get links) ---
from urllib.parse import urlparse

# Импортируем Logger
try:
    from Logger import Logger
    LOGGER_AVAILABLE = True
except ImportError:
    LOGGER_AVAILABLE = False

# Попытка импортировать aiohttp для асинхронных запросов
try:
    import aiohttp
    import ssl
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# Попытка импортировать telethon для отслеживания Telegram
try:
    from telethon import TelegramClient  # type: ignore
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False

# Константы для прогресс-бара
PROGRESS_BAR_WIDTH = 30
# Используем ASCII символы для совместимости с разными кодировками
PROGRESS_BAR_CHAR = "-"  # Можно использовать "=" или "#"

init(autoreset=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройка логирования - все логи в единый файл lorett_monitor.log
log_file_path = '/root/lorett/GroundLinkServer/lorett_monitor.log'
log_dir = os.path.dirname(log_file_path)
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file_path, encoding='utf-8', mode='a')
    ],
    force=True  # Перезаписываем предыдущую конфигурацию
)
logger = logging.getLogger(__name__)
EMAIL_DISABLED = False
EMAIL_DEBUG_RECIPIENT: Optional[str] = None

# === Telegram comm passes ===
TG_API_ID = int(os.getenv("TG_API_ID", "25004944"))
TG_API_HASH = os.getenv("TG_API_HASH", "3d29770555fbca4b0ea880003ed892bc")
TG_CHANNEL = os.getenv("TG_CHANNEL", "https://t.me/+Lb1SuoOUlodhYTli")
TG_SESSION = os.getenv("TG_SESSION", "/root/lorett/GroundLinkServer/telegram")
COMM_DB_PATH = os.getenv("COMM_DB_PATH", "/root/lorett/GroundLinkServer/comm_passes.db")
ALL_PASSES_DB_PATH = os.getenv("ALL_PASSES_DB_PATH", "/root/lorett/GroundLinkServer/all_passes.db")

COMM_STATION_ALIASES = {
    "MUR": "R3.2S_Murmansk",
    "ANA": "R4.6S_Anadyr",
}

COMM_PASS_LINE_RE = re.compile(
    r"^\s*(?P<station>\S+)\s+(?P<satellite>\S+)(?:\s+UTC)?\s+"
    r"(?P<date>\d{4}[./-]\d{2}[./-]\d{2})\s+"
    r"(?P<start>\d{2}:\d{2}:\d{2})\s*-\s*"
    r"(?P<end>\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)


def _all_db_quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _all_db_init(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def _all_db_ensure_station_table(conn: sqlite3.Connection, station: str) -> None:
    table_name = _all_db_quote_identifier(station)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            satellite TEXT NOT NULL,
            session_start TIMESTAMP NOT NULL,
            session_end TIMESTAMP NOT NULL,
            successful TEXT NOT NULL CHECK (successful IN ('Yes', 'No'))
        )
        """
    )
    cursor.execute(f"PRAGMA table_info({table_name})")
    cols = {row[1] for row in cursor.fetchall()}
    if "successful" not in cols:
        cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN successful TEXT NOT NULL DEFAULT 'Yes'"
        )
    conn.commit()
    cursor.close()


def _all_db_upsert_pass(
    conn: sqlite3.Connection,
    station: str,
    satellite: str,
    session_start: str,
    session_end: str,
    successful: str,
) -> str:
    _all_db_ensure_station_table(conn, station)
    table_name = _all_db_quote_identifier(station)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT id, session_end, successful
        FROM {table_name}
        WHERE satellite = ?
          AND session_start = ?
        LIMIT 1
        """,
        (satellite, session_start),
    )
    existing = cursor.fetchone()
    if existing is not None:
        row_id, current_end, current_success = existing
        if current_end != session_end or current_success != successful:
            cursor.execute(
                f"UPDATE {table_name} SET session_end = ?, successful = ? WHERE id = ?",
                (session_end, successful, row_id),
            )
            conn.commit()
            logger.info(
                "all_passes UPDATE station=%s satellite=%s start=%s end=%s successful=%s",
                station,
                satellite,
                session_start,
                session_end,
                successful,
            )
            cursor.close()
            return "updated"
        cursor.close()
        return "exists"
    cursor.execute(
        f"""
        INSERT INTO {table_name} (satellite, session_start, session_end, successful)
        VALUES (?, ?, ?, ?)
        """,
        (satellite, session_start, session_end, successful),
    )
    conn.commit()
    cursor.close()
    logger.info(
        "all_passes INSERT station=%s satellite=%s start=%s end=%s successful=%s",
        station,
        satellite,
        session_start,
        session_end,
        successful,
    )
    return "inserted"


def _all_normalize_data_time(value: str) -> Optional[str]:
    value = value.strip()
    if not value:
        return None
    if "." in value:
        value = value.split(".", 1)[0]
    return value


def _all_parse_log_metadata(
    log_path: Path,
) -> Tuple[Optional[Tuple[str, str, str, str, str]], Optional[str]]:
    station = log_path.parent.name
    satellite = None
    header_station = None
    start_time = None
    first_data_time = None
    last_data_time = None

    try:
        with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("#Station:"):
                    header_station = line.split(":", 1)[1].strip()
                    continue
                if line.startswith("#Satellite:"):
                    satellite = line.split(":", 1)[1].strip()
                    continue
                if line.startswith("#Start time:"):
                    start_time = _all_normalize_data_time(line.split(":", 1)[1])
                    continue
                if line[0].isdigit():
                    parts = line.split()
                    if len(parts) >= 2 and ":" in parts[1]:
                        data_time = f"{parts[0]} {parts[1]}"
                    else:
                        data_time = parts[0]
                    data_time = _all_normalize_data_time(data_time)
                    if data_time:
                        if first_data_time is None:
                            first_data_time = data_time
                        last_data_time = data_time
    except OSError:
        return None, "read_error"

    station = header_station or station
    session_start = start_time or first_data_time
    session_end = last_data_time or session_start

    missing = []
    if not satellite:
        missing.append("missing_satellite")
    if not session_start:
        missing.append("missing_start_time")
    if not session_end:
        missing.append("missing_end_time")
    if missing:
        return None, ",".join(missing)

    bend_type = detect_bend_type_from_header(log_path)
    snr_sum, snr_count = extract_snr_from_log(log_path, bend_type)
    avg_snr = snr_sum / snr_count if snr_count > 0 else 0.0
    if str(satellite).upper() == "TY-42":
        successful = "Yes" if avg_snr > 7.0 else "No"
    else:
        threshold = X_BEND_FAILURE_THRESHOLD if bend_type == "X" else L_BEND_FAILURE_THRESHOLD
        successful = "Yes" if avg_snr > threshold else "No"

    return (station, satellite, session_start, session_end, successful), None


def update_all_passes_db_for_date(target_date: str) -> None:
    year, month, date_str, _ = get_date_paths(target_date)
    base_logs_dir = Path("/root/lorett/GroundLinkServer/logs") / year / month / date_str
    if not base_logs_dir.exists():
        logger.warning("Папка логов не найдена для даты %s: %s", target_date, base_logs_dir)
        return

    conn = _all_db_init(ALL_PASSES_DB_PATH)
    inserted = 0
    updated = 0
    skipped = 0
    try:
        for log_path in base_logs_dir.rglob("*.log"):
            metadata, reason = _all_parse_log_metadata(log_path)
            if metadata is None:
                skipped += 1
                logger.warning("all_passes SKIP %s reason=%s", log_path, reason or "unknown")
                continue
            station, satellite, session_start, session_end, successful = metadata
            action = _all_db_upsert_pass(
                conn, station, satellite, session_start, session_end, successful
            )
            if action == "inserted":
                inserted += 1
            elif action == "updated":
                updated += 1
    finally:
        conn.close()

    logger.info(
        "all_passes sync done for %s: inserted=%s updated=%s skipped=%s",
        target_date,
        inserted,
        updated,
        skipped,
    )


def _list_db_tables(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'"
    )
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables


def _comm_get_day_bounds(target_date: str) -> Tuple[str, str]:
    target_day = datetime.strptime(target_date, "%Y%m%d")
    day_start = target_day.strftime("%Y-%m-%d 00:00:00")
    day_end = target_day.strftime("%Y-%m-%d 23:59:59")
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if now_str < day_end:
        day_end = now_str
    return day_start, day_end


def _comm_is_commercial_satellite(satellite: str, tokens: List[str]) -> bool:
    sat = satellite.upper()
    for token in tokens:
        if not token:
            continue
        if len(token) <= 3:
            if sat.startswith(token) or f"{token}-" in sat:
                return True
        else:
            if token in sat:
                return True
    return False


def _comm_collect_stats(target_date: str) -> Tuple[Dict[str, Dict[str, int]], Dict[str, int]]:
    commercial_tokens = [s.upper() for s in _load_commercial_satellites()]
    comm_conn = sqlite3.connect(COMM_DB_PATH)
    all_conn = sqlite3.connect(ALL_PASSES_DB_PATH)

    stats: Dict[str, Dict[str, int]] = {}
    totals = {"planned": 0, "successful": 0, "not_received": 0}

    try:
        comm_tables = _list_db_tables(comm_conn)
        all_tables = set(_list_db_tables(all_conn))
        day_start, day_end = _comm_get_day_bounds(target_date)

        for station in sorted(comm_tables):
            station_q = _comm_quote_identifier(station)
            comm_cur = comm_conn.cursor()
            comm_cur.execute(
                f"""
                SELECT satellite, session_start, session_end
                FROM {station_q}
                WHERE datetime(session_start) >= datetime(?)
                  AND datetime(session_start) <= datetime(?)
                """,
                (day_start, day_end),
            )
            rows = [
                row
                for row in comm_cur.fetchall()
                if _comm_is_commercial_satellite(str(row[0]), commercial_tokens)
            ]
            comm_cur.close()

            planned = len(rows)
            if planned == 0:
                continue

            successful = 0
            not_received = 0

            if station not in all_tables:
                not_received = planned
            else:
                all_q = _all_db_quote_identifier(station)
                all_cur = all_conn.cursor()
                for satellite, session_start, session_end in rows:
                    all_cur.execute(
                        f"""
                        SELECT successful
                        FROM {all_q}
                        WHERE satellite = ?
                          AND datetime(session_start) <= datetime(?)
                          AND datetime(session_end) >= datetime(?)
                        LIMIT 1
                        """,
                        (satellite, session_start, session_end),
                    )
                    match = all_cur.fetchone()
                    if not match:
                        not_received += 1
                    elif match[0] == "Yes":
                        successful += 1
                    else:
                        not_received += 1
                all_cur.close()

            stats[station] = {
                "planned": planned,
                "successful": successful,
                "not_received": not_received,
            }
            totals["planned"] += planned
            totals["successful"] += successful
            totals["not_received"] += not_received
    finally:
        comm_conn.close()
        all_conn.close()

    return stats, totals


def print_comm_passes_status(target_date: str) -> None:
    """Сверяет запланированные коммерческие пролеты с all_passes.db и печатает статус."""
    stats, totals = _comm_collect_stats(target_date)

    print(f"\n{Fore.CYAN + Style.BRIGHT}СТАТУС КОММЕРЧЕСКИХ ПРОЛЕТОВ  {target_date}")
    print(f"{Fore.CYAN}{'Станция':<30} {'Всего':>10} {'Успешных':>12} {'Не принятых':>12} {'% не принятых':>15}")
    print("-" * 75)

    for station in sorted(stats.keys()):
        planned = stats[station]["planned"]
        successful = stats[station]["successful"]
        not_received = stats[station]["not_received"]
        percent = (not_received / planned * 100) if planned > 0 else 0.0
        print(
            f"{Fore.CYAN}{station:<30} {planned:>10} {successful:>12} {not_received:>12} {percent:>14.1f}%"
        )

    print("-" * 75)
    total_percent = (totals["not_received"] / totals["planned"] * 100) if totals["planned"] > 0 else 0.0
    print(
        f"{Fore.GREEN + Style.BRIGHT}{'ИТОГО':<30} {totals['planned']:>10} "
        f"{totals['successful']:>12} {totals['not_received']:>12} {total_percent:>14.1f}%"
    )
    logger.info(
        "comm_passes status for %s: planned=%s yes=%s not_received=%s",
        target_date,
        totals["planned"],
        totals["successful"],
        totals["not_received"],
    )


def _comm_collect_log_links(target_date: str) -> Dict[str, List[str]]:
    """Собирает ссылки на графики коммерческих пролетов (успешные/неуспешные)."""
    commercial_tokens = [s.upper() for s in _load_commercial_satellites()]
    comm_conn = sqlite3.connect(COMM_DB_PATH)

    try:
        _, _, date_str, _ = get_date_paths(target_date)
        logs_dir = Path("/root/lorett/GroundLinkServer/logs")
        base_logs_dir = logs_dir / date_str[:4] / date_str[4:6] / date_str
        if not base_logs_dir.exists():
            return {"successful": [], "unsuccessful": []}

        day_start, day_end = _comm_get_day_bounds(target_date)
        day_start_dt = datetime.fromisoformat(day_start)
        day_end_dt = datetime.fromisoformat(day_end)

        # Индекс логов по станции
        log_index: Dict[str, List[Dict[str, Any]]] = {}
        for log_path in base_logs_dir.rglob("*.log"):
            metadata, _reason = _all_parse_log_metadata(log_path)
            if metadata is None:
                continue
            station, satellite, session_start, session_end, successful = metadata
            try:
                start_dt = datetime.fromisoformat(session_start)
                end_dt = datetime.fromisoformat(session_end)
            except ValueError:
                continue
            log_index.setdefault(station, []).append(
                {
                    "satellite": satellite,
                    "start": start_dt,
                    "end": end_dt,
                    "successful": successful,
                    "filename": log_path.name,
                }
            )

        # Базовый URL для просмотра графиков
        _, _, base_urls, _headers, _alias_to_canonical = load_config()
        base_url = base_urls.get("reg") or base_urls.get("oper") or "https://eus.lorett.org/eus"

        success_links: set[str] = set()
        fail_links: set[str] = set()

        for station in _list_db_tables(comm_conn):
            station_q = _comm_quote_identifier(station)
            comm_cur = comm_conn.cursor()
            comm_cur.execute(
                f"""
                SELECT satellite, session_start, session_end
                FROM {station_q}
                WHERE datetime(session_start) >= datetime(?)
                  AND datetime(session_start) <= datetime(?)
                """,
                (day_start, day_end),
            )
            rows = [
                row
                for row in comm_cur.fetchall()
                if _comm_is_commercial_satellite(str(row[0]), commercial_tokens)
            ]
            comm_cur.close()

            entries = log_index.get(station, [])
            if not entries:
                continue

            for satellite, session_start, session_end in rows:
                try:
                    start_dt = datetime.fromisoformat(session_start)
                    end_dt = datetime.fromisoformat(session_end)
                except ValueError:
                    continue
                if start_dt < day_start_dt or start_dt > day_end_dt:
                    continue

                match = None
                for entry in entries:
                    if str(entry["satellite"]).upper() != str(satellite).upper():
                        continue
                    if entry["start"] <= start_dt and entry["end"] >= end_dt:
                        match = entry
                        break

                if not match:
                    continue

                log_url = f"{base_url}/log_view/{quote(str(match['filename']))}"
                if match["successful"] == "Yes":
                    success_links.add(log_url)
                else:
                    fail_links.add(log_url)

        return {
            "successful": sorted(success_links),
            "unsuccessful": sorted(fail_links),
        }
    finally:
        comm_conn.close()


def _comm_split_by_double_newline(text: str) -> List[str]:
    chunks = [part.strip() for part in text.split("\n\n")]
    return [part for part in chunks if part]


def _comm_quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _comm_init_db(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def _comm_ensure_station_table(conn: sqlite3.Connection, station: str) -> None:
    table_name = _comm_quote_identifier(station)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            satellite TEXT NOT NULL,
            session_start TIMESTAMP NOT NULL,
            session_end TIMESTAMP NOT NULL
        )
        """
    )
    conn.commit()
    cursor.close()


def _comm_upsert_pass(
    conn: sqlite3.Connection,
    station: str,
    satellite: str,
    session_start: str,
    session_end: str,
) -> str:
    _comm_ensure_station_table(conn, station)
    table_name = _comm_quote_identifier(station)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT id, session_end
        FROM {table_name}
        WHERE satellite = ?
          AND session_start = ?
        LIMIT 1
        """,
        (satellite, session_start),
    )
    existing = cursor.fetchone()
    if existing is not None:
        row_id, current_end = existing
        if current_end != session_end:
            cursor.execute(
                f"UPDATE {table_name} SET session_end = ? WHERE id = ?",
                (session_end, row_id),
            )
            conn.commit()
            logger.info(
                "comm_passes UPDATE station=%s satellite=%s start=%s end=%s",
                station,
                satellite,
                session_start,
                session_end,
            )
            cursor.close()
            return "updated"
        cursor.close()
        return "exists"
    cursor.execute(
        f"INSERT INTO {table_name} (satellite, session_start, session_end) VALUES (?, ?, ?)",
        (satellite, session_start, session_end),
    )
    conn.commit()
    cursor.close()
    logger.info(
        "comm_passes INSERT station=%s satellite=%s start=%s end=%s",
        station,
        satellite,
        session_start,
        session_end,
    )
    return "inserted"


def _comm_parse_passes(text: str) -> List[Tuple[str, str, str, str]]:
    passes: List[Tuple[str, str, str, str]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        match = COMM_PASS_LINE_RE.match(line)
        if not match:
            continue
        station = match.group("station")
        station = COMM_STATION_ALIASES.get(station, station)
        satellite = match.group("satellite")
        date = match.group("date").replace(".", "-").replace("/", "-")
        start_time = match.group("start")
        end_time = match.group("end")
        session_start = f"{date} {start_time}"
        session_end = f"{date} {end_time}"
        passes.append((station, satellite, session_start, session_end))
    return passes


def _comm_insert_from_parts(conn: sqlite3.Connection, parts: List[str]) -> Tuple[int, int]:
    inserted = 0
    updated = 0
    for part in parts:
        for station, satellite, start, end in _comm_parse_passes(part):
            action = _comm_upsert_pass(conn, station, satellite, start, end)
            if action == "inserted":
                inserted += 1
            elif action == "updated":
                updated += 1
    return inserted, updated


async def _comm_start_telegram_client() -> "TelegramClient":
    client: Optional["TelegramClient"] = None
    try:
        client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)
        await client.start()
        return client
    except sqlite3.OperationalError as exc:
        if "database is locked" not in str(exc).lower():
            raise
        if client is not None:
            await client.disconnect()
        fallback_session = f"{TG_SESSION}_pid{os.getpid()}"
        client = TelegramClient(fallback_session, TG_API_ID, TG_API_HASH)
        await client.start()
        return client


async def sync_comm_passes_once() -> None:
    if not TELETHON_AVAILABLE:
        raise SystemExit("telethon не установлен. Установите: pip install telethon")
    if not TG_API_ID or not TG_API_HASH or not TG_CHANNEL:
        raise SystemExit("Set TG_API_ID, TG_API_HASH, TG_CHANNEL")

    logger.info("Синхронизация Telegram канала: %s", TG_CHANNEL)
    client = await _comm_start_telegram_client()
    db_conn = _comm_init_db(COMM_DB_PATH)

    try:
        total_msgs = 0
        total_inserted = 0
        total_updated = 0
        async for msg in client.iter_messages(TG_CHANNEL, reverse=True):
            total_msgs += 1
            text = msg.message or ""
            parts = _comm_split_by_double_newline(text)
            inserted, updated = _comm_insert_from_parts(db_conn, parts)
            total_inserted += inserted
            total_updated += updated
        logger.info(
            "Синхронизация завершена: сообщений=%s, вставлено=%s, обновлено=%s",
            total_msgs,
            total_inserted,
            total_updated,
        )
    finally:
        db_conn.close()
        await client.disconnect()


def _run_comm_passes_sync() -> None:
    if not TELETHON_AVAILABLE:
        logger.warning("telethon не установлен, синхронизация Telegram отключена")
        return
    if not TG_API_ID or not TG_API_HASH or not TG_CHANNEL:
        logger.warning("TG_API_ID/TG_API_HASH/TG_CHANNEL не заданы, синхронизация Telegram отключена")
        return
    try:
        asyncio.run(sync_comm_passes_once())
    except Exception as exc:
        logger.error(f"Ошибка синхронизации Telegram: {exc}", exc_info=True)


def _log_config_full() -> None:
    """Логирует config.json полностью (без изменений)."""
    try:
        config_path = Path("/root/lorett/GroundLinkServer/config.json")
        if not config_path.exists():
            logger.warning(f"config.json не найден: {config_path}")
            return
        with open(config_path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        # Пишем как есть (без маскировки)
        logger.info("config.json (full): %s", raw)
    except Exception as e:
        logger.error(f"Не удалось залогировать config.json: {e}", exc_info=True)


def _load_debug_email_from_config() -> Optional[str]:
    try:
        config_path = Path("/root/lorett/GroundLinkServer/config.json")
        if not config_path.exists():
            return None
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        if not isinstance(config, dict):
            return None
        email_cfg = config.get("email", {})
        if not isinstance(email_cfg, dict):
            return None
        return email_cfg.get("debug_recipient")
    except Exception:
        return None


# === Константы (загружаются из config.json с fallback на значения по умолчанию) ===
# Значения по умолчанию используются если конфиг не найден или параметры отсутствуют
_DEFAULT_CONSTANTS = {
    'min_log_file_size': 100,
    'min_avg_snr_threshold': 4.0,
    'x_bend_failure_threshold': 3.85,
    'l_bend_failure_threshold': 0.0,
    'request_timeout': 30,
    'max_concurrent_downloads': 10,
    'graph_viewport_width': 620,
    'graph_viewport_height': 680,
    'graph_load_delay': 0.5,
    'graph_scroll_x': 0,
    'graph_scroll_y': 0
}

# Валидирует значения констант
def _validate_constants(constants: Dict[str, Any]) -> Dict[str, Any]:
    """Валидирует значения констант из конфига."""
    validated = {}
    for key, value in constants.items():
        if key.endswith('_threshold') or key.endswith('_size'):
            if not isinstance(value, (int, float)) or value < 0:
                raise ValueError(f"Константа {key} должна быть неотрицательным числом, получено: {value}")
            validated[key] = float(value) if isinstance(value, (int, float)) else value
        elif key == 'max_concurrent_downloads':
            if not isinstance(value, int) or value < 1 or value > 100:
                raise ValueError(f"{key} должна быть целым числом от 1 до 100, получено: {value}")
            validated[key] = value
        elif key.endswith('_width') or key.endswith('_height'):
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"Константа {key} должна быть положительным целым числом, получено: {value}")
            validated[key] = value
        elif key.endswith('_scroll_x') or key.endswith('_scroll_y'):
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"Константа {key} должна быть неотрицательным целым числом, получено: {value}")
            validated[key] = value
        elif key.endswith('_delay'):
            if not isinstance(value, (int, float)) or value < 0:
                raise ValueError(f"Константа {key} должна быть неотрицательным числом, получено: {value}")
            validated[key] = float(value)
        elif key == 'request_timeout':
            if not isinstance(value, (int, float)) or value < 1:
                raise ValueError(f"{key} должна быть положительным числом, получено: {value}")
            validated[key] = float(value)
        else:
            validated[key] = value
    return validated

# Загружаем константы из конфига
def _load_constants() -> Dict[str, Any]:
    """Загружает и валидирует константы из config.json или использует значения по умолчанию."""
    try:
        config_path = Path('/root/lorett/GroundLinkServer/config.json')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                constants = config.get('constants', {})
                # Объединяем с дефолтными значениями (конфиг имеет приоритет)
                result = {**_DEFAULT_CONSTANTS, **constants}
                # Валидируем значения
                result = _validate_constants(result)
                logger.debug(f"Константы загружены из config.json: {result}")
                return result
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Не удалось загрузить константы из config.json: {e}. Используются значения по умолчанию.")
    except ValueError as e:
        logger.error(f"Ошибка валидации констант: {e}. Используются значения по умолчанию.")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке констант: {e}", exc_info=True)
    return _DEFAULT_CONSTANTS.copy()


def _load_commercial_satellites() -> List[str]:
    """Загружает список коммерческих спутников из config.json (fallback на дефолт)."""
    default = ["TY", "2025-108B", "2024-110A", "TEE-04A", "TEE-01B", "JILIN-1_GAOFEN_4A"]
    try:
        config_path = Path("/root/lorett/GroundLinkServer/config.json")
        if not config_path.exists():
            return default
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        satellites = config.get("commercial_satellites", default)
        if not isinstance(satellites, list):
            return default
        cleaned = [str(s).strip() for s in satellites if str(s).strip()]
        return cleaned or default
    except Exception:
        return default

# Инициализируем константы при импорте модуля
_CONSTANTS = _load_constants()

# Экспортируем константы как переменные модуля
MIN_LOG_FILE_SIZE = _CONSTANTS['min_log_file_size']
MIN_AVG_SNR_THRESHOLD = _CONSTANTS['min_avg_snr_threshold']
X_BEND_FAILURE_THRESHOLD = _CONSTANTS['x_bend_failure_threshold']
L_BEND_FAILURE_THRESHOLD = _CONSTANTS['l_bend_failure_threshold']
REQUEST_TIMEOUT = _CONSTANTS['request_timeout']
MAX_CONCURRENT_DOWNLOADS = _CONSTANTS['max_concurrent_downloads']
GRAPH_VIEWPORT_WIDTH = _CONSTANTS['graph_viewport_width']
GRAPH_VIEWPORT_HEIGHT = _CONSTANTS['graph_viewport_height']
GRAPH_LOAD_DELAY = _CONSTANTS['graph_load_delay']
GRAPH_SCROLL_X = _CONSTANTS['graph_scroll_x']
GRAPH_SCROLL_Y = _CONSTANTS['graph_scroll_y']


# Создает SSL контекст без проверки сертификатов для асинхронных запросов
def create_unverified_ssl_context():
    """
    Создает SSL контекст с отключенной проверкой сертификатов.
    
    Returns:
        ssl.SSLContext: SSL контекст с отключенной проверкой
    """
    if not AIOHTTP_AVAILABLE:
        return None
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return ssl_context


# Возвращает пути для даты: year, month, date_str, date_display
def get_date_paths(target_date: str) -> Tuple[str, str, str, str]:
    """
    Возвращает пути для даты: year, month, date_str, date_display.
    
    Args:
        target_date: Дата в формате YYYYMMDD
        
    Returns:
        Tuple[str, str, str, str]: (year, month, date_str, date_display)
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d")
    return (
        date_obj.strftime("%Y"),
        date_obj.strftime("%m"),
        target_date,
        date_obj.strftime("%d.%m.%Y")
    )


def _load_email_defaults_from_test_email() -> Dict[str, Any]:
    """
    Пытается загрузить SMTP-настройки по умолчанию из test_email.py.
    Нужен для обратной совместимости и быстрого старта (как просили: "используя данные из test_email.py").
    """
    try:
        import test_email as te  # type: ignore
        return {
            "smtp_server": getattr(te, "SMTP_SERVER", None),
            "smtp_port": getattr(te, "SMTP_PORT", None),
            "sender_email": getattr(te, "SENDER_EMAIL", None),
            "sender_password": getattr(te, "SENDER_PASSWORD", None),
            "recipient_email": getattr(te, "RECIPIENT_EMAIL", None),
            "subject": getattr(te, "EMAIL_SUBJECT", None),
        }
    except Exception:
        return {}


def get_email_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Возвращает настройки email.

    Приоритет источников:
    - config.json -> email.*
    - переменные окружения
    - значения по умолчанию из test_email.py
    """
    defaults = _load_email_defaults_from_test_email()
    email_cfg = (config or {}).get("email", {}) if isinstance(config, dict) else {}

    enabled_raw = email_cfg.get("enabled", os.getenv("EMAIL_ENABLED"))
    if enabled_raw is None:
        enabled = True  # по умолчанию включаем (пользователь попросил сделать отправку)
    elif isinstance(enabled_raw, bool):
        enabled = enabled_raw
    else:
        enabled = str(enabled_raw).strip().lower() in ("1", "true", "yes", "y", "on")

    # Параметры SMTP
    smtp_server = (
        email_cfg.get("smtp_server")
        or os.getenv("SMTP_SERVER")
        or defaults.get("smtp_server")
        or "smtp.yandex.ru"
    )
    smtp_port = int(
        email_cfg.get("smtp_port")
        or os.getenv("SMTP_PORT")
        or defaults.get("smtp_port")
        or 465
    )
    sender_email = (
        email_cfg.get("sender_email")
        or os.getenv("SENDER_EMAIL")
        or defaults.get("sender_email")
        or ""
    )
    sender_password = (
        email_cfg.get("sender_password")
        or os.getenv("SENDER_PASSWORD")
        or defaults.get("sender_password")
        or ""
    )

    # Получатели: одна строка, можно через запятую/точку с запятой
    recipient_raw = (
        email_cfg.get("recipient_email")
        or email_cfg.get("to")
        or os.getenv("RECIPIENT_EMAIL")
        or defaults.get("recipient_email")
        or ""
    )
    recipients = [r.strip() for r in re.split(r"[;,]", str(recipient_raw)) if r.strip()]

    # CC (копия): можно строкой "a@x.ru, b@y.ru" или списком ["a@x.ru", "b@y.ru"]
    cc_raw = (
        email_cfg.get("cc")
        or email_cfg.get("cc_emails")
        or os.getenv("EMAIL_CC")
        or ""
    )
    if isinstance(cc_raw, (list, tuple, set)):
        cc_recipients = [str(r).strip() for r in cc_raw if str(r).strip()]
    else:
        cc_recipients = [r.strip() for r in re.split(r"[;,]", str(cc_raw)) if r.strip()]

    subject = (
        email_cfg.get("subject")
        or os.getenv("EMAIL_SUBJECT")
        or defaults.get("subject")
        or "Ежедневное письмо"
    )

    attach_report_raw = email_cfg.get("attach_report", os.getenv("EMAIL_ATTACH_REPORT", "1"))
    attach_report = True if attach_report_raw is None else str(attach_report_raw).strip().lower() in ("1", "true", "yes", "y", "on")

    recipients_final = recipients
    cc_final = cc_recipients
    if EMAIL_DEBUG_RECIPIENT:
        recipients_final = [EMAIL_DEBUG_RECIPIENT]
        cc_final = []

    return {
        "enabled": enabled,
        "smtp_server": smtp_server,
        "smtp_port": smtp_port,
        "sender_email": sender_email,
        "sender_password": sender_password,
        "recipients": recipients_final,
        "cc_recipients": cc_final,
        "subject": subject,
        "attach_report": attach_report,
    }


def _parse_station_summary_file(summary_path: Path) -> Optional[Tuple[int, int]]:
    """
    Парсит файл avg_snr_<station>.txt и возвращает (total_files, unsuccessful_passes).
    Формат пишется в analyze_downloaded_logs().
    """
    try:
        if not summary_path.exists():
            return None
        total_files: Optional[int] = None
        unsuccessful: Optional[int] = None
        with open(summary_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("Всего файлов обработано:"):
                    m = re.search(r"(\d+)", line)
                    if m:
                        total_files = int(m.group(1))
                # Поддерживаем оба варианта формулировки
                elif line.startswith("Неуспешных пролетов:") or line.startswith("Пустых пролетов:"):
                    m = re.search(r"(\d+)", line)
                    if m:
                        unsuccessful = int(m.group(1))
        if total_files is None or unsuccessful is None:
            return None
        return total_files, unsuccessful
    except Exception as e:
        logger.debug(f"Не удалось распарсить {summary_path}: {e}")
        return None


def _compute_overall_unsuccessful_for_date(
    *,
    date_yyyymmdd: str,
    stations: Dict[str, str],
    station_bend_map: Dict[str, str],
) -> Tuple[int, int]:
    """
    Возвращает (total_files_all, unsuccessful_all) по всем станциям за дату.
    Сначала пытается читать ранее сохраненные avg_snr_*.txt, иначе считает по логам.
    """
    year, month, date_str, _ = get_date_paths(date_yyyymmdd)
    base_logs_dir = Path("/root/lorett/GroundLinkServer/logs") / year / month / date_str
    if not base_logs_dir.exists():
        return 0, 0

    total_all = 0
    unsuccessful_all = 0

    for station_name in stations.keys():
        station_folder = base_logs_dir / station_name
        if not station_folder.exists():
            continue

        # Быстрый путь: берем из summary файла, если он есть
        summary_path = station_folder / f"avg_snr_{station_folder.name}.txt"
        parsed = _parse_station_summary_file(summary_path)
        if parsed:
            total_files, unsuccessful = parsed
            total_all += int(total_files)
            unsuccessful_all += int(unsuccessful)
            continue

        # Fallback: считаем по логам
        bend_type = station_bend_map.get(station_name)
        bend_type_upper = (bend_type or "L").upper()
        threshold = X_BEND_FAILURE_THRESHOLD if bend_type_upper == "X" else L_BEND_FAILURE_THRESHOLD

        log_files = sorted(station_folder.glob("*.log"))
        if not log_files:
            continue

        total_files = 0
        unsuccessful = 0
        for log_file in log_files:
            snr_sum, count = extract_snr_from_log(log_file, bend_type)
            avg_snr = snr_sum / count if count > 0 else 0.0
            total_files += 1
            if avg_snr <= threshold:
                unsuccessful += 1

        total_all += total_files
        unsuccessful_all += unsuccessful

    return total_all, unsuccessful_all


def generate_overall_unsuccessful_7d_chart(
    *,
    target_date: str,
    stations: Dict[str, str],
    station_bend_map: Dict[str, str],
    output_path: Path,
    days: int = 7,
) -> Optional[Path]:
    """
    Генерирует PNG график общего процента пустых пролетов за последние N дней (включая target_date).
    Возвращает путь к PNG или None (если не удалось сгенерировать).
    """
    try:
        # matplotlib может отсутствовать — тогда просто пропускаем
        import matplotlib

        matplotlib.use("Agg")  # без GUI
        import matplotlib.pyplot as plt  # type: ignore
        # Не засоряем лог INFO-сообщениями matplotlib
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

        date_obj = datetime.strptime(target_date, "%Y%m%d")
        points: List[Tuple[str, Optional[float]]] = []

        for i in range(days - 1, -1, -1):
            d = date_obj - timedelta(days=i)
            d_str = d.strftime("%Y%m%d")
            label = d.strftime("%d.%m")

            total_all, unsuccessful_all = _compute_overall_unsuccessful_for_date(
                date_yyyymmdd=d_str,
                stations=stations,
                station_bend_map=station_bend_map,
            )
            if total_all <= 0:
                points.append((label, None))
            else:
                points.append((label, (unsuccessful_all / total_all) * 100.0))

        labels = [p[0] for p in points]
        # Для линейного графика: пропуски данных делаем разрывами (NaN)
        values = [(p[1] if p[1] is not None else float("nan")) for p in points]

        fig = plt.figure(figsize=(10, 3.2), dpi=150)
        ax = fig.add_subplot(111)
        x = list(range(len(labels)))
        ax.plot(
            x,
            values,
            color="#1976d2",
            linewidth=2,
            marker="o",
            markersize=4,
        )
        ax.set_xticks(x, labels)
        ax.set_ylim(0, 100)
        ax.set_yticks(list(range(0, 101, 10)))
        ax.set_ylabel("% пустых")
        ax.set_title("Общий % пустых пролетов за последние 7 дней (все станции)")
        ax.grid(axis="y", linestyle="--", alpha=0.3)

        # Подписи значений
        for idx, (lbl, v) in enumerate(points):
            if v is None:
                ax.text(x[idx], 0.5, "нет\nданных", ha="center", va="bottom", fontsize=7, color="#616161")
            else:
                ax.text(x[idx], min(99.5, v + 1.5), f"{v:.1f}%", ha="center", va="bottom", fontsize=7, color="#212121")

        fig.tight_layout()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, bbox_inches="tight")
        plt.close(fig)
        return output_path
    except ImportError:
        logger.warning("matplotlib не установлен — сводный график за 7 дней не будет добавлен в письмо")
        return None
    except Exception as e:
        logger.warning(f"Не удалось сгенерировать сводный график за 7 дней: {e}", exc_info=True)
        return None


def generate_comm_unsuccessful_7d_chart(
    *,
    target_date: str,
    output_path: Path,
    days: int = 7,
) -> Optional[Path]:
    """
    Генерирует PNG график процента неуспешных коммерческих пролетов за последние N дней.
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

        date_obj = datetime.strptime(target_date, "%Y%m%d")
        points: List[Tuple[str, Optional[float]]] = []

        for i in range(days - 1, -1, -1):
            d = date_obj - timedelta(days=i)
            d_str = d.strftime("%Y%m%d")
            stats, totals = _comm_collect_stats(d_str)
            if totals["planned"] > 0:
                percent = totals["not_received"] / totals["planned"] * 100
                points.append((d.strftime("%d.%m"), percent))
            else:
                points.append((d.strftime("%d.%m"), None))

        labels = [p[0] for p in points]
        values = [(p[1] if p[1] is not None else float("nan")) for p in points]
        x = list(range(len(labels)))

        fig = plt.figure(figsize=(10, 3.2), dpi=150)
        ax = fig.add_subplot(111)
        ax.plot(
            x,
            values,
            color="#ff9f0a",
            linewidth=2,
            marker="o",
            markersize=4,
        )
        ax.set_xticks(x, labels)
        ax.set_ylim(0, 100)
        ax.set_yticks(list(range(0, 101, 10)))
        ax.set_ylabel("% не принятых")
        ax.set_title("Коммерческие пролеты: % не принятых за последние 7 дней")
        ax.grid(axis="y", linestyle="--", alpha=0.3)

        for idx, (lbl, v) in enumerate(points):
            if v is None:
                ax.text(x[idx], 0.5, "нет\nданных", ha="center", va="bottom", fontsize=7, color="#616161")
            else:
                ax.text(x[idx], min(99.5, v + 1.5), f"{v:.1f}%", ha="center", va="bottom", fontsize=7, color="#212121")

        fig.tight_layout()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, bbox_inches="tight")
        plt.close(fig)
        return output_path
    except ImportError:
        logger.warning("matplotlib не установлен — коммерческий график за 7 дней не будет добавлен в письмо")
        return None
    except Exception as e:
        logger.warning(f"Не удалось сгенерировать коммерческий график за 7 дней: {e}", exc_info=True)
        return None


def build_stats_email_body(
    target_date: str,
    all_results: Dict[str, Dict[str, Any]],
    graphs_dir: Optional[Path] = None,
    summary_7d_chart_path: Optional[Path] = None,
    comm_stats: Optional[Dict[str, Dict[str, int]]] = None,
    comm_totals: Optional[Dict[str, int]] = None,
    comm_summary_7d_chart_path: Optional[Path] = None,
    comm_links: Optional[Dict[str, List[str]]] = None,
) -> Tuple[str, Dict[str, Path]]:
    """
    Формирует HTML таблицу статистики для письма и собирает графики для встраивания.
    
    Returns:
        Tuple[str, Dict[str, Path]]: (HTML тело письма, словарь {cid: путь_к_графику})
    """
    # Форматируем дату в формат DD.MM.YYYY
    date_display = f"{target_date[6:8]}.{target_date[4:6]}.{target_date[0:4]}"
    
    # Начинаем HTML документ
    html_lines = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1.0'>",
        "<style>",
        "  * { box-sizing: border-box; }",
        "  body {",
        "    font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'SF Pro Text', 'Helvetica Neue', Helvetica, Arial, sans-serif;",
        "    font-size: 14px;",
        "    line-height: 1.6;",
        "    color: #1d1d1f;",
        "    background-color: #f5f5f7;",
        "    margin: 0;",
        "    padding: 10px 6px;",
        "    -webkit-text-size-adjust: 100%;",
        "    -ms-text-size-adjust: 100%;",
        "  }",
        "  .container {",
        "    max-width: 820px;",
        "    margin: 0 auto;",
        "    background-color: #ffffff;",
        "    border-radius: 12px;",
        "    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);",
        "    overflow: hidden;",
        "  }",
        "  .header {",
        "    padding: 16px 14px 12px;",
        "    border-bottom: 1px solid #e5e5e7;",
        "    background: linear-gradient(to bottom, #ffffff, #fafafa);",
        "  }",
        "  h2 {",
        "    font-size: 18px;",
        "    font-weight: 600;",
        "    letter-spacing: -0.5px;",
        "    color: #1d1d1f;",
        "    margin: 0 0 8px 0;",
        "  }",
        "  .date {",
        "    font-size: 15px;",
        "    color: #86868b;",
        "    font-weight: 400;",
        "    margin: 0;",
        "  }",
        "  .content {",
        "    padding: 10px;",
        "  }",
        "  .table-wrap {",
        "    width: 100%;",
        "    overflow-x: auto;",
        "    -webkit-overflow-scrolling: touch;",
        "  }",
        "  .adaptive-table {",
        "    width: 100%;",
        "    min-width: 560px;",
        "    border-collapse: separate;",
        "    border-spacing: 0;",
        "    background-color: #ffffff;",
        "    border-radius: 12px;",
        "    overflow: hidden;",
        "    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);",
        "  }",
        "  .adaptive-table thead { background-color: #f5f5f7; }",
        "  .adaptive-table th {",
        "    padding: 12px 14px;",
        "    text-align: left;",
        "    font-size: 12px;",
        "    font-weight: 600;",
        "    color: #86868b;",
        "    text-transform: uppercase;",
        "    letter-spacing: 0.5px;",
        "    border-bottom: 1px solid #e5e5e7;",
        "    border-right: 1px solid #e5e5e7;",
        "    white-space: nowrap;",
        "  }",
        "  .adaptive-table th:last-child { border-right: none; }",
        "  .adaptive-table th.number { text-align: right; }",
        "  .adaptive-table td {",
        "    padding: 12px 14px;",
        "    border-bottom: 1px solid #f5f5f7;",
        "    border-right: 1px solid #e5e5e7;",
        "    font-size: 14px;",
        "    color: #1d1d1f;",
        "    white-space: nowrap;",
        "  }",
        "  .adaptive-table td:last-child { border-right: none; }",
        "  .adaptive-table tr:last-child td { border-bottom: none; }",
        "  .adaptive-table tr:hover { background-color: #fafafa; }",
        "  .adaptive-table .number { text-align: right; font-variant-numeric: tabular-nums; }",
        "  .adaptive-table .total-row { background-color: #f5f5f7; font-weight: 600; }",
        "  .adaptive-table .total-row td { border-top: 2px solid #e5e5e7; }",
        "  .adaptive-table .row-good { background-color: #dcfce7; }",
        "  .adaptive-table .row-warning { background-color: #fef3c7; }",
        "  .adaptive-table .row-error { background-color: #fee2e2; }",
        "  .summary-table {",
        "    width: 100%;",
        "    border-collapse: separate;",
        "    border-spacing: 0;",
        "    background-color: #ffffff;",
        "    border-radius: 12px;",
        "    overflow: hidden;",
        "    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);",
        "  }",
        "  .summary-table thead {",
        "    background-color: #f5f5f7;",
        "  }",
        "  .summary-table th {",
        "    padding: 16px 20px;",
        "    text-align: left;",
        "    font-size: 13px;",
        "    font-weight: 600;",
        "    color: #86868b;",
        "    text-transform: uppercase;",
        "    letter-spacing: 0.5px;",
        "    border-bottom: 1px solid #e5e5e7;",
        "    border-right: 1px solid #e5e5e7;",
        "  }",
        "  .summary-table th:last-child { border-right: none; }",
        "  .summary-table th.number { text-align: right; }",
        "  .summary-table td {",
        "    padding: 16px 20px;",
        "    border-bottom: 1px solid #f5f5f7;",
        "    border-right: 1px solid #e5e5e7;",
        "    font-size: 15px;",
        "    color: #1d1d1f;",
        "  }",
        "  .summary-table td:last-child { border-right: none; }",
        "  .summary-table tr:last-child td { border-bottom: none; }",
        "  .summary-table tr:hover {",
        "    background-color: #fafafa;",
        "  }",
        "  .summary-table .row-good {",
        "    background-color: #dcfce7;",
        "  }",
        "  .summary-table .row-good:hover {",
        "    background-color: #dcfce7;",
        "  }",
        "  .summary-table .row-warning {",
        "    background-color: #fef3c7;",
        "  }",
        "  .summary-table .row-warning:hover {",
        "    background-color: #fef3c7;",
        "  }",
        "  .summary-table .row-error {",
        "    background-color: #fee2e2;",
        "  }",
        "  .summary-table .row-error:hover {",
        "    background-color: #fee2e2;",
        "  }",
        "  .summary-table .number {",
        "    text-align: right;",
        "    font-variant-numeric: tabular-nums;",
        "  }",
        "  .summary-table .total-row {",
        "    background-color: #f5f5f7;",
        "    font-weight: 600;",
        "  }",
        "  .summary-table .total-row td {",
        "    border-top: 2px solid #e5e5e7;",
        "    padding-top: 20px;",
        "    padding-bottom: 20px;",
        "  }",
        "  /* Вертикальная 'карточка' станции внутри одной ячейки */",
        "  .station-name {",
        "    font-weight: 600;",
        "    font-size: 15px;",
        "    margin: 0 0 8px 0;",
        "  }",
        "  .metrics-table {",
        "    width: 100%;",
        "    border-collapse: collapse;",
        "  }",
        "  .metrics-table td {",
        "    padding: 6px 0;",
        "    border: none;",
        "    border-bottom: 1px solid #f0f0f2;",
        "    font-size: 14px;",
        "  }",
        "  .metrics-table tr:last-child td { border-bottom: none; }",
        "  .metrics-label {",
        "    color: #86868b;",
        "    font-size: 11px;",
        "    font-weight: 600;",
        "    text-transform: uppercase;",
        "    letter-spacing: 0.5px;",
        "  }",
        "  .metrics-value {",
        "    text-align: right;",
        "    font-variant-numeric: tabular-nums;",
        "  }",
        "",
        "  /* Десктоп-таблица (изначальная, 5 колонок) */",
        "  .desktop-table {",
        "    width: 100%;",
        "    border-collapse: separate;",
        "    border-spacing: 0;",
        "    background-color: #ffffff;",
        "    border-radius: 12px;",
        "    overflow: hidden;",
        "    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);",
        "  }",
        "  .desktop-table thead { background-color: #f5f5f7; }",
        "  .desktop-table th {",
        "    padding: 16px 20px;",
        "    text-align: left;",
        "    font-size: 13px;",
        "    font-weight: 600;",
        "    color: #86868b;",
        "    text-transform: uppercase;",
        "    letter-spacing: 0.5px;",
        "    border-bottom: 1px solid #e5e5e7;",
        "    border-right: 1px solid #e5e5e7;",
        "  }",
        "  .desktop-table th:last-child { border-right: none; }",
        "  .desktop-table th.number { text-align: right; }",
        "  .desktop-table td {",
        "    padding: 16px 20px;",
        "    border-bottom: 1px solid #f5f5f7;",
        "    border-right: 1px solid #e5e5e7;",
        "    font-size: 15px;",
        "    color: #1d1d1f;",
        "  }",
        "  .desktop-table td:last-child { border-right: none; }",
        "  .desktop-table tr:last-child td { border-bottom: none; }",
        "  .desktop-table tr:hover { background-color: #fafafa; }",
        "  .desktop-table .number { text-align: right; font-variant-numeric: tabular-nums; }",
        "  .desktop-table .total-row { background-color: #f5f5f7; font-weight: 600; }",
        "  .desktop-table .total-row td { border-top: 2px solid #e5e5e7; }",
        "  .desktop-table .row-good { background-color: #dcfce7; }",
        "  .desktop-table .row-warning { background-color: #fef3c7; }",
        "  .desktop-table .row-error { background-color: #fee2e2; }",
        "  .graph-section {",
        "    margin-top: 18px;",
        "    padding: 8px 6px;",
        "    background-color: #fafafa;",
        "    border-radius: 12px;",
        "    page-break-inside: avoid;",
        "  }",
        "  .graph-title {",
        "    font-size: 17px;",
        "    font-weight: 600;",
        "    letter-spacing: -0.3px;",
        "    color: #1d1d1f;",
        "    margin-bottom: 14px;",
        "  }",
        "  .graph-image {",
        "    max-width: 100%;",
        "    height: auto;",
        "    border-radius: 8px;",
        "    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);",
        "    display: block;",
        "  }",
        "  .empty-message {",
        "    color: #86868b;",
        "    font-style: italic;",
        "    font-size: 13px;",
        "    padding: 12px 0;",
        "  }",
        "  .unsuccessful-list {",
        "    margin-top: 16px;",
        "    padding: 14px;",
        "    background-color: #fff5f5;",
        "    border-radius: 8px;",
        "    border-left: 3px solid #ff3b30;",
        "  }",
        "  .successful-list {",
        "    margin-top: 16px;",
        "    padding: 14px;",
        "    background-color: #ecfdf5;",
        "    border-radius: 8px;",
        "    border-left: 3px solid #2e7d32;",
        "  }",
        "  .unsuccessful-list strong {",
        "    color: #ff3b30;",
        "    font-size: 15px;",
        "    font-weight: 600;",
        "    display: block;",
        "    margin-bottom: 12px;",
        "  }",
        "  .successful-list strong {",
        "    color: #2e7d32;",
        "    font-size: 15px;",
        "    font-weight: 600;",
        "    display: block;",
        "    margin-bottom: 12px;",
        "  }",
        "  .unsuccessful-list ul {",
        "    margin: 0;",
        "    padding-left: 20px;",
        "    color: #1d1d1f;",
        "    font-size: 14px;",
        "  }",
        "  .successful-list ul {",
        "    margin: 0;",
        "    padding-left: 20px;",
        "    color: #1d1d1f;",
        "    font-size: 14px;",
        "  }",
        "  .unsuccessful-list li {",
        "    margin-bottom: 6px;",
        "  }",
        "  .chart-container {",
        "    margin-top: 24px;",
        "    padding: 8px 6px;",
        "    background-color: #fafafa;",
        "    border-radius: 12px;",
        "  }",
        "  /* Адаптивные отступы (без media-query, используем фиксированные значения) */",
        "  body { padding: 10px 6px; }",
        "  .container { border-radius: 10px; }",
        "  .header { padding: 16px 14px 12px; }",
        "  .content { padding: 10px; }",
        "  .graph-section { padding: 8px 6px; }",
        "  .chart-container { padding: 8px 6px; }",
        "</style>",
        "</head>",
        "<body>",
        "<div class='container'>",
        "  <div class='header'>",
        f"    <h2>Сводка по станциям {date_display}</h2>",
        "  </div>",
        "  <div class='content'>"
    ]

    # Словарь для хранения графиков: {cid: путь_к_файлу}
    inline_images = {}

    # Коммерческие пролеты (таблица) — выводим первым блоком
    if comm_stats is not None and comm_totals is not None:
        html_lines.append("    <h2 style='margin-top: 0; font-size: 24px; font-weight: 600; letter-spacing: -0.3px; color: #1d1d1f;'>Коммерческие пролеты</h2>")
        html_lines.append("    <div class='table-wrap'>")
        html_lines.append("      <table class='adaptive-table'>")
        html_lines.append("        <thead>")
        html_lines.append("          <tr>")
        html_lines.append("            <th>Станция</th>")
        html_lines.append("            <th class='number'>Всего</th>")
        html_lines.append("            <th class='number'>Успешных</th>")
        html_lines.append("            <th class='number'>Не принятых</th>")
        html_lines.append("            <th class='number'>% не принятых</th>")
        html_lines.append("          </tr>")
        html_lines.append("        </thead>")
        html_lines.append("        <tbody>")

        for station_name in sorted(comm_stats.keys()):
            stats = comm_stats[station_name]
            planned = int(stats.get("planned", 0))
            successful = int(stats.get("successful", 0))
            not_received = int(stats.get("not_received", 0))
            percent = (not_received / planned * 100) if planned > 0 else 0.0
            station_name_escaped = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            html_lines.append("        <tr>")
            html_lines.append(f"          <td>{station_name_escaped}</td>")
            html_lines.append(f"          <td class='number'>{planned}</td>")
            html_lines.append(f"          <td class='number'>{successful}</td>")
            html_lines.append(f"          <td class='number'>{not_received}</td>")
            html_lines.append(f"          <td class='number'>{percent:.1f}%</td>")
            html_lines.append("        </tr>")

        total_planned = int(comm_totals.get("planned", 0))
        total_successful = int(comm_totals.get("successful", 0))
        total_not_received = int(comm_totals.get("not_received", 0))
        total_percent = (total_not_received / total_planned * 100) if total_planned > 0 else 0.0
        html_lines.append("        <tr class='total-row'>")
        html_lines.append("          <td>ИТОГО</td>")
        html_lines.append(f"          <td class='number'>{total_planned}</td>")
        html_lines.append(f"          <td class='number'>{total_successful}</td>")
        html_lines.append(f"          <td class='number'>{total_not_received}</td>")
        html_lines.append(f"          <td class='number'>{total_percent:.1f}%</td>")
        html_lines.append("        </tr>")
        html_lines.append("        </tbody>")
        html_lines.append("      </table>")
        html_lines.append("    </div>")

        if comm_summary_7d_chart_path and Path(comm_summary_7d_chart_path).exists():
            comm_cid = "comm_unsuccessful_7d"
            inline_images[comm_cid] = Path(comm_summary_7d_chart_path)
            html_lines.append("    <div class='chart-container'>")
            html_lines.append(f"      <img src='cid:{comm_cid}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='Коммерческие пролеты: % не принятых за 7 дней' />")
            html_lines.append("    </div>")

        if comm_links:
            success_links = comm_links.get("successful") or []
            fail_links = comm_links.get("unsuccessful") or []
            html_lines.append("    <div class='graph-section'>")
            html_lines.append("      <div class='graph-title'>Графики коммерческих пролетов</div>")
            if success_links:
                html_lines.append("      <div class='successful-list'>")
                html_lines.append("        <strong>Успешные коммерческие пролеты:</strong>")
                html_lines.append("        <ul>")
                for url in success_links:
                    html_lines.append(f"          <li><a href='{url}'>{url}</a></li>")
                html_lines.append("        </ul>")
                html_lines.append("      </div>")
            if fail_links:
                html_lines.append("      <div class='unsuccessful-list'>")
                html_lines.append("        <strong>Неуспешные:</strong>")
                html_lines.append("        <ul>")
                for url in fail_links:
                    html_lines.append(f"          <li><a href='{url}'>{url}</a></li>")
                html_lines.append("        </ul>")
                html_lines.append("      </div>")
            html_lines.append("    </div>")

    # Общая статистика по станциям
    html_lines.append("    <h2 style='margin-top: 48px; font-size: 24px; font-weight: 600; letter-spacing: -0.3px; color: #1d1d1f;'>Общая статистика</h2>")
    html_lines.append("    <div class='table-wrap'>")
    html_lines.append("      <table class='adaptive-table'>")
    html_lines.append("        <thead>")
    html_lines.append("          <tr>")
    html_lines.append("            <th>Станция</th>")
    html_lines.append("            <th class='number'>Всего</th>")
    html_lines.append("            <th class='number'>Успешных</th>")
    html_lines.append("            <th class='number'>Пустых</th>")
    html_lines.append("            <th class='number'>% пустых</th>")
    html_lines.append("          </tr>")
    html_lines.append("        </thead>")
    html_lines.append("        <tbody>")

    # Сортируем станции по среднему SNR (как в консоли)
    sorted_stations = sorted(all_results.items(), key=lambda x: x[1].get('avg_snr', 0), reverse=True)

    for station_name, stats in sorted_stations:
        files = int(stats.get("files", 0) or 0)
        successful = int(stats.get("successful_passes", 0) or 0)
        unsuccessful = int(stats.get("unsuccessful_passes", 0) or 0)
        unsuccessful_percent = (unsuccessful / files * 100) if files > 0 else 0.0
        
        # Определяем класс строки для цветовой подсветки
        if files == 0:
            row_class = "row-error"  # Нет пролетов - красный
        elif unsuccessful_percent < 2:
            row_class = "row-good"  # 0-2% - зеленый
        elif unsuccessful_percent < 20:
            row_class = "row-warning"  # 2-20% - желтый
        else:
            row_class = "row-error"  # 20% и более - красный
        
        # Экранируем HTML специальные символы
        station_name_escaped = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        
        html_lines.append(f"        <tr class='{row_class}'>")
        html_lines.append(f"          <td>{station_name_escaped}</td>")
        html_lines.append(f"          <td class='number'>{files}</td>")
        html_lines.append(f"          <td class='number'>{successful}</td>")
        html_lines.append(f"          <td class='number'>{unsuccessful}</td>")
        html_lines.append(f"          <td class='number'>{unsuccessful_percent:.1f}%</td>")
        html_lines.append("        </tr>")

    # Итоговые значения
    total_all_files = sum(int(s.get("files", 0) or 0) for s in all_results.values())
    total_successful = sum(int(s.get("successful_passes", 0) or 0) for s in all_results.values())
    total_unsuccessful = sum(int(s.get("unsuccessful_passes", 0) or 0) for s in all_results.values())
    total_unsuccessful_percent = (total_unsuccessful / total_all_files * 100) if total_all_files > 0 else 0.0

    # Добавляем итоговую строку
    html_lines.append("        <tr class='total-row'>")
    html_lines.append("          <td>ИТОГО</td>")
    html_lines.append(f"          <td class='number'>{total_all_files}</td>")
    html_lines.append(f"          <td class='number'>{total_successful}</td>")
    html_lines.append(f"          <td class='number'>{total_unsuccessful}</td>")
    html_lines.append(f"          <td class='number'>{total_unsuccessful_percent:.1f}%</td>")
    html_lines.append("        </tr>")

    # Закрываем таблицу
    html_lines.extend([
        "        </tbody>",
        "      </table>",
        "    </div>",
    ])

    if summary_7d_chart_path and Path(summary_7d_chart_path).exists():
        # Fallback на PNG, если точки не передали
        summary_cid = "summary_unsuccessful_7d"
        inline_images[summary_cid] = Path(summary_7d_chart_path)
        html_lines.append("    <div class='chart-container'>")
        html_lines.append(f"      <img src='cid:{summary_cid}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='Сводный график за 7 дней' />")
        html_lines.append("    </div>")
    else:
        html_lines.append("    <p class='empty-message'>Нет данных для построения графика.</p>")

    # Добавляем графики после таблицы
    if graphs_dir and graphs_dir.exists():
        html_lines.append("    <h2 style='margin-top: 48px; font-size: 24px; font-weight: 600; letter-spacing: -0.3px; color: #1d1d1f;'>Графики пролетов</h2>")
        
        for station_name, stats in sorted_stations:
            # Экранируем HTML специальные символы
            station_name_escaped = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            
            html_lines.append(f"    <div class='graph-section'>")
            html_lines.append(f"      <div class='graph-title'>{station_name_escaped}</div>")
            
            max_snr_filename = stats.get('max_snr_filename', '')
            if max_snr_filename:
                # Формируем имя файла графика
                graph_name = max_snr_filename.replace('.log', '.png').replace(' ', '_')
                graph_path = graphs_dir / graph_name
                
                if graph_path.exists():
                    # Создаем уникальный CID для встроенного изображения
                    cid = f"graph_{station_name}_{graph_name}".replace(' ', '_').replace('.', '_')
                    inline_images[cid] = graph_path
                    
                    html_lines.append(f"      <img src='cid:{cid}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='График для {station_name_escaped}' />")
                else:
                    html_lines.append(f"      <p class='empty-message'>График не найден. Станция не работает</p>")
            else:
                html_lines.append(f"      <p class='empty-message'>График не найден. Станция не работает</p>")
            
            # Добавляем список пустых пролетов, если они есть
            unsuccessful_filenames = stats.get('unsuccessful_filenames', [])
            if unsuccessful_filenames:
                html_lines.append(f"      <div class='unsuccessful-list'>")
                html_lines.append(f"        <strong>Пустые пролеты ({len(unsuccessful_filenames)})</strong>")
                html_lines.append(f"        <ul>")
                for filename in unsuccessful_filenames:
                    # Вместо имени файла показываем ссылку на просмотр лога (график)
                    log_url = f"https://eus.lorett.org/eus/log_view/{quote(str(filename))}"
                    # Экранируем HTML специальные символы
                    log_url_escaped = log_url.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    html_lines.append(
                        f"          <li><a href='{log_url_escaped}' target='_blank' rel='noopener noreferrer'>{log_url_escaped}</a></li>"
                    )
                html_lines.append(f"        </ul>")
                html_lines.append(f"      </div>")
            
            html_lines.append(f"    </div>")
    
    # Закрываем HTML
    html_lines.extend([
        "  </div>",
        "</div>",
        "</body>",
        "</html>"
    ])

    return "\n".join(html_lines), inline_images


def send_stats_email(
    *,
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: str,
    recipients: List[str],
    cc_recipients: Optional[List[str]] = None,
    subject: str,
    body: str,
    attachments: Optional[List[Path]] = None,
    inline_images: Optional[Dict[str, Path]] = None,
) -> bool:
    """
    Отправляет письмо со статистикой через SMTP (SSL на 465, STARTTLS на 587).
    
    Args:
        inline_images: Словарь {cid: путь_к_изображению} для встроенных изображений
    """
    if not sender_email or not sender_password or not recipients:
        logger.warning("Email: не заданы sender/password/recipients — отправка пропущена")
        return False

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipients)
    if cc_recipients:
        msg["Cc"] = ", ".join([r for r in cc_recipients if r])
    msg["Subject"] = subject
    # Определяем, является ли body HTML (проверяем наличие HTML тегов)
    is_html = body.strip().startswith("<!DOCTYPE html>") or body.strip().startswith("<html>")
    msg.attach(MIMEText(body, "html" if is_html else "plain", "utf-8"))

    # Добавляем встроенные изображения (inline attachments)
    if inline_images:
        for cid, image_path in inline_images.items():
            try:
                if not image_path or not Path(image_path).exists():
                    logger.warning(f"Email: график не найден {image_path}")
                    continue
                with open(image_path, "rb") as f:
                    img = MIMEImage(f.read())
                img.add_header('Content-ID', f'<{cid}>')
                img.add_header('Content-Disposition', 'inline', filename=Path(image_path).name)
                msg.attach(img)
            except Exception as e:
                logger.warning(f"Email: не удалось приложить график {image_path}: {e}")

    # Добавляем обычные вложения
    for p in attachments or []:
        try:
            if not p or not Path(p).exists():
                continue
            with open(p, "rb") as f:
                part = MIMEApplication(f.read(), Name=Path(p).name)
            part["Content-Disposition"] = f'attachment; filename="{Path(p).name}"'
            msg.attach(part)
        except Exception as e:
            logger.warning(f"Email: не удалось приложить файл {p}: {e}")

    try:
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port), timeout=30)
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port), timeout=30)
            if int(smtp_port) == 587:
                server.starttls()
        server.login(sender_email, sender_password)
        # Важно: Cc должны попасть и в заголовок, и в список реальных получателей SMTP
        all_recipients: List[str] = list(recipients or [])
        if cc_recipients:
            all_recipients.extend([r for r in cc_recipients if r])
        server.send_message(msg, from_addr=sender_email, to_addrs=all_recipients)
        server.quit()
        return True
    except Exception as e:
        logger.warning(f"Email: ошибка отправки: {e}", exc_info=True)
        return False


# Выводит сообщение об ошибке в консоль с цветным форматированием и логированием
def print_error(message: str, is_critical: bool = False, exc_info: bool = False) -> None:
    """
    Выводит сообщение об ошибке в едином формате и логирует его.
    
    Args:
        message: Текст сообщения об ошибке
        is_critical: Если True, сообщение выводится с ярким стилем и логируется как ERROR
        exc_info: Если True, логирует полный traceback
    """
    style = Fore.RED + (Style.BRIGHT if is_critical else Style.NORMAL)
    print(f"{style}{message}{Style.RESET_ALL}")
    
    # Логируем ошибку
    if is_critical:
        logger.error(message, exc_info=exc_info)
    else:
        logger.warning(message, exc_info=exc_info)


# Обновляет визуальный прогресс-бар загрузки файлов
def update_progress_bar(current: int, total: int) -> None:
    """
    Обновляет прогресс-бар с безопасным выводом для разных кодировок.
    
    Args:
        current: Текущее значение прогресса
        total: Общее значение прогресса
    """
    if total == 0:
        return
    
    percentage = int(100 * current / total)
    filled = int(PROGRESS_BAR_WIDTH * current / total)
    bar = PROGRESS_BAR_CHAR * filled + " " * (PROGRESS_BAR_WIDTH - filled)
    
    try:
        sys.stdout.write(f"\r{bar} {percentage:3d}%")
        sys.stdout.flush()
    except (UnicodeEncodeError, AttributeError):
        # Fallback для консолей с проблемами кодировки
        sys.stdout.write(f"\rProgress: {percentage:3d}%")
        sys.stdout.flush()

# Проверяет валидность лог-файла (отсеивает ошибки БД и пустые файлы)
def validate_log_file_detailed(file_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Проверяет, является ли файл валидным лог-файлом и возвращает детали ошибки.
    
    Args:
        file_path: Путь к файлу для проверки
        
    Returns:
        Tuple[bool, Optional[str]]: (True, None) если файл валиден, (False, описание_ошибки) если содержит ошибки
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read().strip()
            
        # Проверка на ошибки базы данных
        if content.startswith('ERROR: No log') and 'in the database' in content:
            # Извлекаем полное сообщение об ошибке
            error_lines = content.split('\n')[:3]  # Первые 3 строки для контекста
            error_msg = ' '.join(error_lines).strip()
            return False, f"Ошибка базы данных: {error_msg}"
            
        # Проверка на другие ошибки (маленький файл с ERROR)
        if len(content) < MIN_LOG_FILE_SIZE and ('ERROR' in content.upper() or 'error' in content.lower()):
            # Извлекаем строки с ошибками
            error_lines = [line for line in content.split('\n') if 'error' in line.lower()][:3]
            error_msg = ' | '.join(error_lines).strip() if error_lines else content[:200]
            return False, f"Файл содержит ошибку (размер {len(content)} байт): {error_msg}"
        
        # Проверка на пустой файл
        if len(content) == 0:
            return False, "Файл пустой"
            
        return True, None
    except (FileNotFoundError, PermissionError, IOError, OSError) as e:
        # Если не удалось прочитать файл, считаем его валидным (консервативный подход)
        logger.debug(f"Не удалось прочитать файл {file_path} для валидации: {e}")
        return True, None


def is_valid_log_file(file_path: Path) -> bool:
    """
    Проверяет, является ли файл валидным лог-файлом.
    
    Args:
        file_path: Путь к файлу для проверки
        
    Returns:
        bool: True если файл валиден, False если содержит ошибки
    """
    is_valid, _ = validate_log_file_detailed(file_path)
    return is_valid


def is_log_file_downloaded(file_path: Path) -> bool:
    """
    Проверяет, что лог-файл успешно загружен и валиден.
    
    Args:
        file_path: Путь к файлу для проверки
        
    Returns:
        bool: True если файл загружен и валиден, False в противном случае
    """
    try:
        # Проверяем существование файла
        if not file_path.exists():
            return False
        
        # Проверяем, что файл не пустой
        if file_path.stat().st_size == 0:
            return False
        
        # Проверяем валидность содержимого
        if not is_valid_log_file(file_path):
            return False
        
        return True
    except Exception:
        return False


# Загружает конфигурацию из config.json и возвращает станции, URL и заголовки
def load_config() -> Tuple[
    Dict[str, str],          # canonical station -> type (kept for compatibility)
    List[str],               # station identifiers to search on pages (canonical + aliases)
    Dict[str, str],          # base_urls
    Dict[str, str],          # headers
    Dict[str, str],          # alias_to_canonical
]:
    """
    Загружает конфигурацию из файла config.json.
    
    Returns:
        Tuple содержащий:
        - Словарь {каноническое_имя_станции: тип_станции}
        - Список имён станций для поиска на странице oper (включая aliases)
        - Список имён станций для поиска на странице reg (включая aliases)
        - Список имён станций для поиска на странице frames (включая aliases)
        - Словарь базовых URL для типов станций
        - Словарь HTTP заголовков
        - Словарь алиасов: {alias: canonical_name}
        
    Raises:
        FileNotFoundError: Если файл config.json не найден
        ValueError: Если файл содержит невалидный JSON или некорректную структуру
        RuntimeError: При других ошибках загрузки конфигурации
    """
    config_path = Path('/root/lorett/GroundLinkServer/config.json')
    
    if not config_path.exists():
        raise FileNotFoundError(f"Конфигурационный файл {config_path} не найден")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Ошибка парсинга JSON в config.json: {e}")
    except Exception as e:
        raise RuntimeError(f"Ошибка чтения файла конфигурации: {e}")
    
    try:
        # Типы станций (oper/reg) больше не используются для загрузки (мы парсим обе страницы).
        # Сохраняем словарь как "известные станции" по имени.
        stations = {s["name"]: "station" for s in config["stations"]}

        # Поддержка "aliases" для исторических имен станций в логах + auto-alias:
        # auto-alias = часть до первого "_" (например R3.2S_Murmansk -> R3.2S).
        alias_to_canonical: Dict[str, str] = {}
        for s in config["stations"]:
            canonical = s["name"]
            alias_to_canonical[canonical] = canonical

            # explicit aliases
            aliases = s.get("aliases") or s.get("alias") or []
            if isinstance(aliases, str):
                aliases = [aliases]
            if isinstance(aliases, (list, tuple, set)):
                for a in aliases:
                    a = str(a).strip()
                    if not a:
                        continue
                    alias_to_canonical[a] = canonical

            # auto-alias
            auto = str(canonical).split("_", 1)[0].strip()
            if auto and auto not in alias_to_canonical:
                alias_to_canonical[auto] = canonical
        base_urls = config.get('base_urls', {
            'oper': "https://eus.lorett.org/eus",
            'reg': "http://eus.lorett.org/eus",
            'frames': "https://eus.lorett.org/eus"
        })
        headers = config.get('headers', {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        station_identifiers = sorted(set(alias_to_canonical.keys()))
        return stations, station_identifiers, base_urls, headers, alias_to_canonical
    except KeyError as e:
        raise ValueError(f"Отсутствует обязательное поле в конфигурации: {e}")
    except Exception as e:
        raise RuntimeError(f"Ошибка обработки конфигурации: {e}")


# Синхронно загружает HTML страницу со списком логов для типа станций
def fetch_logs_page(
    url: str,
    st: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
) -> str:
    """
    Загружает HTML страницу со списком логов для указанного типа станций.
    Автоматически повторяет запрос при ошибке 503 (Service Unavailable).
    
    Args:
        url: URL страницы со списком логов
        st: Тип станций ('oper', 'reg', 'frames')
        headers: HTTP заголовки для запроса
        max_retries: Максимальное количество попыток (по умолчанию 3)
        
    Returns:
        str: HTML содержимое страницы
        
    Raises:
        requests.RequestException: При ошибках HTTP запроса после всех попыток
    """
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT, verify=False)
            r.raise_for_status()
            return r.text
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 503:
                last_error = e
                # Экспоненциальная задержка: 2, 4, 8 секунд
                delay = 2 ** attempt
                logger.warning(f"Ошибка 503 (Service Unavailable) для '{st}', попытка {attempt}/{max_retries}. Повтор через {delay} сек...")
                if attempt < max_retries:
                    time.sleep(delay)
                    continue
                logger.error(f"Превышено количество попыток для '{st}' после {max_retries} попыток")
                raise
            else:
                # Другие HTTP ошибки пробрасываем сразу
                raise
        except requests.RequestException as e:
            # Сетевые ошибки (timeout, connection error) - пробрасываем сразу
            raise
    
    # Если дошли сюда, значит все попытки исчерпаны
    if last_error:
        raise last_error
    raise requests.RequestException(f"Не удалось загрузить страницу для '{st}' после {max_retries} попыток")


# --- test_1.py-style parsing: station columns + direct log_get links ---
_STATION_RE = re.compile(r"logstation\.html\?stid=([^&\"']+)", re.I)
_DATE_ROW_RE = re.compile(
    r"<tr>\s*<td[^>]*>\s*<b>\s*(\d{4}-\d{2}-\d{2})\s*</b>\s*</td>(.*?)</tr>",
    re.I | re.S,
)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.I | re.S)
_PASS_RE = re.compile(
    r"href=['\"](log_view/[^'\"]+)['\"].*?"
    r"href=['\"](log_get/[^'\"]+)['\"]",
    re.I | re.S,
)


def _parse_log_get_links_for_date(
    *,
    html: str,
    page_url: str,
    date_iso: str,
    station_allowlist: Optional[set] = None,
    alias_to_canonical: Optional[Dict[str, str]] = None,
) -> Dict[str, List[str]]:
    """
    Возвращает {canonical_station: [log_get_url, ...]} за конкретную дату (YYYY-MM-DD),
    используя таблицу на странице как в GroundLinkMonitorServer/test/test_1.py.
    """
    # станции на странице = порядок колонок
    local: List[str] = []
    for m in _STATION_RE.finditer(html):
        st = m.group(1)
        if st not in local:
            local.append(st)

    allow = station_allowlist
    out: Dict[str, set] = defaultdict(set)  # canonical -> set(url)

    for row in _DATE_ROW_RE.finditer(html):
        if row.group(1) != date_iso:
            continue
        cells = _TD_RE.findall(row.group(2))
        for i, cell in enumerate(cells):
            if i >= len(local):
                break
            station_id = local[i]
            if allow is not None and station_id not in allow:
                continue
            canonical = alias_to_canonical.get(station_id, station_id) if alias_to_canonical else station_id
            for p in _PASS_RE.finditer(cell):
                get_url = urljoin(page_url, p.group(2))
                out[canonical].add(get_url)

    return {k: sorted(v) for k, v in out.items()}


# Асинхронно скачивает один лог-файл с валидацией и ограничением параллелизма через семафор
async def download_single_log_async(
    session: 'aiohttp.ClientSession',
    log_get_url: str,
    file_path: Path,
    headers: Dict[str, str],
    semaphore: asyncio.Semaphore,
    max_retries: int = 2
) -> Tuple[bool, Optional[str], int]:
    """
    Асинхронно скачивает один лог-файл с повторными попытками.
    
    Args:
        session: aiohttp клиентская сессия
        log_get_url: Полный URL для скачивания (log_get/...)
        file_path: Путь для сохранения файла
        headers: HTTP заголовки
        semaphore: Семафор для ограничения количества одновременных запросов
        max_retries: Максимальное количество попыток загрузки
        
    Returns:
        Tuple[bool, Optional[str], int]: (успешно ли скачан, сообщение об ошибке если есть, размер файла)
    """
    async with semaphore:
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                ssl_context = create_unverified_ssl_context()
                url = log_get_url
                timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                
                async with session.get(url, headers=headers, timeout=timeout, ssl=ssl_context) as response:
                    response.raise_for_status()
                    content = await response.read()
                    content_size = len(content)
                    
                    # Сохраняем содержимое во временный файл для проверки
                    temp_file = file_path.with_suffix('.tmp')
                    temp_file.write_bytes(content)
                    
                    # Проверяем содержимое файла на наличие ошибок
                    is_valid, error_detail = validate_log_file_detailed(temp_file)
                    if not is_valid:
                        # Файл содержит ошибку - не сохраняем его
                        try:
                            temp_file.unlink()
                        except (FileNotFoundError, PermissionError) as e:
                            logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")
                        last_error = error_detail if error_detail else "Лог содержит ошибки"
                        if attempt < max_retries:
                            await asyncio.sleep(1)  # Небольшая задержка перед повтором
                            continue
                        return False, last_error, 0
                    
                    # Файл валиден - переименовываем временный файл в постоянный
                    try:
                        temp_file.replace(file_path)
                    except (OSError, PermissionError) as e:
                        logger.error(f"Не удалось переместить файл {temp_file} в {file_path}: {e}")
                        last_error = f"Ошибка сохранения файла: {e}"
                        if attempt < max_retries:
                            await asyncio.sleep(1)
                            continue
                        return False, last_error, 0
                    
                    # Проверяем, что файл действительно загружен
                    if is_log_file_downloaded(file_path):
                        return True, None, content_size
                    else:
                        last_error = "Файл не прошел проверку после загрузки"
                        if attempt < max_retries:
                            logger.warning(f"Попытка {attempt}/{max_retries} загрузки {file_path.name} не удалась: {last_error}")
                            await asyncio.sleep(1)
                            continue
                        return False, last_error, 0
                    
            except asyncio.TimeoutError:
                last_error = "Таймаут запроса"
                if attempt < max_retries:
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {file_path.name}: {last_error}")
                    await asyncio.sleep(1)
                    continue
                return False, last_error, 0
            except aiohttp.ClientResponseError as e:
                # Специальная обработка для 503 Service Unavailable
                if e.status == 503:
                    delay = 2 ** attempt  # Экспоненциальная задержка: 2, 4, 8 секунд
                    last_error = f"Ошибка 503 (Service Unavailable)"
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {file_path.name}: {last_error}. Повтор через {delay} сек...")
                    if attempt < max_retries:
                        await asyncio.sleep(delay)
                        continue
                    return False, last_error, 0
                else:
                    last_error = f"Ошибка HTTP {e.status}: {e.message}"
                    if attempt < max_retries:
                        logger.warning(f"Попытка {attempt}/{max_retries} загрузки {file_path.name}: {last_error}")
                        await asyncio.sleep(1)
                        continue
                    return False, last_error, 0
            except aiohttp.ClientError as e:
                last_error = f"Ошибка HTTP: {e}"
                if attempt < max_retries:
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {file_path.name}: {last_error}")
                    await asyncio.sleep(1)
                    continue
                return False, last_error, 0
            except (OSError, PermissionError) as e:
                last_error = f"Ошибка файловой системы: {e}"
                if attempt < max_retries:
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {file_path.name}: {last_error}")
                    await asyncio.sleep(1)
                    continue
                return False, last_error, 0
            except Exception as e:
                last_error = f"Неожиданная ошибка: {e}"
                if attempt < max_retries:
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {file_path.name}: {last_error}")
                    await asyncio.sleep(1)
                    continue
                return False, last_error, 0
        
        return False, last_error or "Превышено максимальное количество попыток", 0


# Асинхронно скачивает все логи параллельно (до MAX_CONCURRENT_DOWNLOADS одновременно) с прогресс-баром
async def download_logs_async(
    all_logs: Dict[str, List[str]],
    stations: Dict[str, str],
    base_urls: Dict[str, str],
    headers: Dict[str, str],
    logs_dir: Path
) -> Tuple[int, int, int]:
    """
    Асинхронно скачивает все логи из словаря all_logs.
    
    Args:
        all_logs: Словарь {имя_станции: [log_get_url, ...]}
        stations: Словарь {имя_станции: тип_станции}
        base_urls: Словарь базовых URL для типов станций (не используется, оставлен для совместимости)
        headers: HTTP заголовки
        logs_dir: Базовая директория для сохранения логов
        
    Returns:
        Tuple[int, int, int]: (количество скачанных файлов, количество ошибок, количество логов "нет в БД")
    """
    if not AIOHTTP_AVAILABLE:
        print_error("aiohttp не установлен. Установите: pip install aiohttp", is_critical=True)
        return 0, 0, 0
    
    downloaded = 0
    failed = 0
    db_missing = 0
    
    ssl_context = create_unverified_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    # Создаем семафор для ограничения количества одновременных запросов
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    # Подготавливаем список всех файлов для загрузки
    files_to_download = []
    existing_files_count = 0
    
    for station_name, log_get_urls in sorted(all_logs.items()):
        # Проверка существования станции в конфиге
        if station_name not in stations:
            continue
        
        station_dir = logs_dir / station_name
        station_dir.mkdir(exist_ok=True)
        
        for log_get_url in sorted(log_get_urls):
            filename = os.path.basename(urlparse(log_get_url).path)
            file_path = station_dir / filename
            
            # Проверяем существующий файл
            if file_path.exists():
                if is_valid_log_file(file_path):
                    existing_files_count += 1
                    continue
                else:
                    # Файл содержит ошибку - удаляем его
                    try:
                        file_path.unlink()
                    except (FileNotFoundError, PermissionError) as e:
                        logger.warning(f"Не удалось удалить невалидный файл {file_path}: {e}")
            
            files_to_download.append((log_get_url, file_path))
    
    total_to_download = len(files_to_download)
    total_files = total_to_download + existing_files_count
    
    # Выводим информацию о загрузке перед прогресс-баром
    if existing_files_count > 0 or total_to_download > 0:
        print(f"{Fore.CYAN}Загрузка: {existing_files_count} файлов уже существует, {total_to_download} файлов к загрузке")
    
    # Инициализируем прогресс-бар
    current_progress = existing_files_count
    progress_lock = asyncio.Lock()
    if total_files > 0:
        update_progress_bar(current_progress, total_files)
    
    async def download_with_progress(
        session: 'aiohttp.ClientSession',
        log_get_url: str,
        file_path: Path,
        headers: Dict[str, str],
        semaphore: asyncio.Semaphore,
    ) -> Tuple[bool, Optional[str], int]:
        """Обертка для обновления прогресс-бара"""
        nonlocal current_progress
        result = await download_single_log_async(session, log_get_url, file_path, headers, semaphore)
        async with progress_lock:
            current_progress += 1
            if total_files > 0:
                update_progress_bar(current_progress, total_files)
        return result
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Выполняем все задачи параллельно
        if files_to_download:
            # Создаем задачи с сессией
            tasks = [
                download_with_progress(session, log_get_url, file_path, headers, semaphore)
                for log_get_url, file_path in files_to_download
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Обрабатываем результаты
            for (_, file_path), result in zip(files_to_download, results):
                if isinstance(result, Exception):
                    failed += 1
                else:
                    success, error_msg, file_size = result
                    if success:
                        downloaded += 1
                    else:
                        failed += 1
                        # Частый кейс для старых дат: ссылка есть, но EUS отвечает "No log ... in the database"
                        if error_msg and "Ошибка базы данных:" in str(error_msg):
                            db_missing += 1
    
    # Завершаем прогресс-бар
    if total_files > 0:
        print()  # Переход на новую строку после прогресс-бара
    
    downloaded += existing_files_count
    
    return downloaded, failed, db_missing


# Главная функция загрузки: находит и скачивает все логи за указанную дату со всех станций
def download_logs_for_date(target_date: str) -> None:
    """
    Скачивает логи для указанной даты со всех станций.
    
    Args:
        target_date: Дата в формате YYYYMMDD
    """
    try:
        stations, station_identifiers, base_urls, headers, alias_to_canonical = load_config()
    except FileNotFoundError as e:
        logger.error(f"Файл config.json не найден: {e}")
        print_error(f"Ошибка загрузки config.json: {e}", is_critical=True)
        return
    except (ValueError, RuntimeError) as e:
        logger.error(f"Ошибка загрузки config.json: {e}")
        print_error(f"Ошибка загрузки config.json: {e}", is_critical=True)
        return
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке конфигурации: {e}", exc_info=True)
        print_error(f"Неожиданная ошибка при загрузке конфигурации: {e}", is_critical=True, exc_info=True)
        return
    
    # Валидация и преобразование даты из YYYYMMDD в YYYY-MM-DD для параметров запроса
    try:
        date_obj = datetime.strptime(target_date, "%Y%m%d")
    except ValueError:
        print_error(f"Неверный формат даты '{target_date}'. Ожидается формат YYYYMMDD (например, 20251208)", is_critical=True)
        return
    
    # Запрашиваем диапазон ±1 день для гарантированного получения данных
    date_before = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
    date_after = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
    date_iso = date_obj.strftime("%Y-%m-%d")

    params = {"t0": date_before, "t1": date_after}
    allow = set(station_identifiers) if station_identifiers else None

    # БЕЗ разделения на oper/reg: парсим обе страницы как в test_1.py
    sources: List[Tuple[str, str]] = []
    reg_base = base_urls.get("reg") or base_urls.get("oper") or "https://eus.lorett.org/eus"
    oper_base = base_urls.get("oper") or base_urls.get("reg") or "https://eus.lorett.org/eus"
    sources.append(("logs_list", urljoin(reg_base.rstrip("/") + "/", "logs_list.html")))
    sources.append(("logs", urljoin(oper_base.rstrip("/") + "/", "logs.html")))
    # frames оставляем опционально: на некоторых инсталляциях формат другой
    frames_base = base_urls.get("frames")
    if frames_base:
        sources.append(("frames", urljoin(frames_base.rstrip("/") + "/", "loglist_frames.html")))

    all_logs: Dict[str, List[str]] = {}
    for label, url in sources:
        try:
            html = fetch_logs_page(url, label, headers, params=params)
            parsed = _parse_log_get_links_for_date(
                html=html,
                page_url=url,
                date_iso=date_iso,
                station_allowlist=allow,
                alias_to_canonical=alias_to_canonical,
            )
            for station, get_urls in parsed.items():
                all_logs.setdefault(station, []).extend(get_urls)
        except requests.RequestException as e:
            logger.warning(f"Ошибка сети при загрузке страницы '{label}': {e}")
            print_error(f"Ошибка при загрузке страницы '{label}': {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при обработке страницы '{label}': {e}", exc_info=True)
            print_error(f"Неожиданная ошибка при обработке страницы '{label}': {e}")
    
    total = sum(len(logs) for logs in all_logs.values())
    logger.info(f"Найдено {total} логов на {len(all_logs)} станциях для даты {target_date}")
    print(f"{Fore.CYAN + Style.BRIGHT}\nСТАТИСТИКА")
    print(f"{Fore.CYAN}Найдено: {total} логов на {len(all_logs)} станциях")
    for station, logs in sorted(all_logs.items()):
        logger.debug(f"Станция {station}: {len(logs)} логов")
        print(f"{Fore.CYAN}  {station}: {len(logs)}")
    
    if not all_logs:
        print(f"{Fore.YELLOW}Логи не найдены!")
        return
    
    # Создаем папку для логов в формате logs\YYYY\MM\YYYYMMDD
    year, month, date_str, _ = get_date_paths(target_date)
    logs_dir = Path('/root/lorett/GroundLinkServer/logs') / year / month / date_str
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"{Fore.CYAN + Style.BRIGHT}\nСКАЧИВАНИЕ")
    
    # Засекаем время начала загрузки
    start_time = time.time()
    
    # Используем асинхронную загрузку, если доступен aiohttp
    if AIOHTTP_AVAILABLE:
        try:
            downloaded, failed, db_missing = asyncio.run(
                download_logs_async(all_logs, stations, base_urls, headers, logs_dir)
            )
        except RuntimeError:
            # Если event loop уже запущен, используем синхронную версию как fallback
            logger.warning("Event loop уже запущен, используется синхронная загрузка")
            downloaded, failed, db_missing = _download_logs_sync(
                all_logs, stations, base_urls, headers, logs_dir, total
            )
    else:
        # Если aiohttp недоступен, используем синхронную загрузку
        logger.warning("aiohttp не установлен, используется синхронная загрузка")
        downloaded, failed, db_missing = _download_logs_sync(
            all_logs, stations, base_urls, headers, logs_dir, total
        )
    
    # Засекаем время окончания загрузки
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Форматируем время
    if elapsed_time < 60:
        time_str = f"{elapsed_time:.2f} сек"
    elif elapsed_time < 3600:
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        time_str = f"{minutes} мин {seconds:.2f} сек"
    else:
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = elapsed_time % 60
        time_str = f"{hours} ч {minutes} мин {seconds:.2f} сек"
    
    if db_missing > 0:
        logger.info(
            f"Загрузка завершена: скачано {downloaded}, ошибок {failed} (нет в БД: {db_missing}), всего {total}, время {time_str}"
        )
    else:
        logger.info(f"Загрузка завершена: скачано {downloaded}, ошибок {failed}, всего {total}, время {time_str}")
    print(f"{Fore.CYAN + Style.BRIGHT}\nРЕЗУЛЬТАТЫ")
    if db_missing > 0:
        print(
            f"{Fore.GREEN if failed == 0 else Fore.RED}Скачано: {downloaded}, Ошибок: {failed} (нет в БД: {db_missing}), Всего: {total}"
        )
    else:
        print(f"{Fore.GREEN if failed == 0 else Fore.RED}Скачано: {downloaded}, Ошибок: {failed}, Всего: {total}")
    print(f"{Fore.CYAN}Время загрузки: {time_str}")
    
    # Вычисляем среднюю скорость загрузки
    if elapsed_time > 0 and downloaded > 0:
        avg_speed = downloaded / elapsed_time
        logger.debug(f"Средняя скорость загрузки: {avg_speed:.2f} файлов/сек")
        print(f"{Fore.CYAN}Средняя скорость: {avg_speed:.2f} файлов/сек")


# Синхронная загрузка логов последовательно (fallback если aiohttp недоступен)
def _download_logs_sync(
    all_logs: Dict[str, List[str]],
    stations: Dict[str, str],
    base_urls: Dict[str, str],
    headers: Dict[str, str],
    logs_dir: Path,
    total: int
) -> Tuple[int, int, int]:
    """
    Синхронная загрузка логов.
    
    Args:
        all_logs: Словарь {имя_станции: [log_get_url, ...]}
        stations: Словарь {имя_станции: тип_станции}
        base_urls: Словарь базовых URL для типов станций (не используется, оставлен для совместимости)
        headers: HTTP заголовки
        logs_dir: Базовая директория для сохранения логов
        total: Общее количество логов
        
    Returns:
        Tuple[int, int, int]: (количество скачанных файлов, количество ошибок, количество логов "нет в БД")
    """
    downloaded = failed = 0
    db_missing = 0
    
    # Подготавливаем список всех файлов для загрузки
    all_files_to_download = []
    existing_files_count = 0
    
    for station_name, log_get_urls in sorted(all_logs.items()):
        # Проверка существования станции в конфиге
        if station_name not in stations:
            continue
        
        station_dir = logs_dir / station_name
        station_dir.mkdir(exist_ok=True)
        
        for log_get_url in sorted(log_get_urls):
            filename = os.path.basename(urlparse(log_get_url).path)
            file_path = station_dir / filename
            if file_path.exists():
                # Проверяем содержимое существующего файла
                if is_valid_log_file(file_path):
                    existing_files_count += 1
                    continue
                else:
                    # Файл содержит ошибку - удаляем его
                    file_path.unlink()
            
            all_files_to_download.append((log_get_url, file_path))
    
    total_to_download = len(all_files_to_download)
    total_files = total_to_download + existing_files_count
    
    # Выводим информацию о загрузке перед прогресс-баром
    if existing_files_count > 0 or total_to_download > 0:
        print(f"{Fore.CYAN}Загрузка: {existing_files_count} файлов уже существует, {total_to_download} файлов к загрузке")
    
    # Инициализируем прогресс-бар
    current_progress = existing_files_count
    if total_files > 0:
        update_progress_bar(current_progress, total_files)
    
    # Загружаем файлы с повторными попытками
    max_retries = 2
    for log_get_url, file_path in all_files_to_download:
        print(f"{Fore.CYAN}Загрузка: {file_path.name}")
        success = False
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                r = requests.get(log_get_url, headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
                r.raise_for_status()
                
                # Сохраняем содержимое во временный файл для проверки
                temp_file = file_path.with_suffix('.tmp')
                temp_file.write_bytes(r.content)
                
                # Проверяем содержимое файла на наличие ошибок
                is_valid, error_detail = validate_log_file_detailed(temp_file)
                if not is_valid:
                    # Файл содержит ошибку - не сохраняем его
                    try:
                        temp_file.unlink()
                    except (FileNotFoundError, PermissionError) as e:
                        logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")
                    last_error = error_detail if error_detail else "Лог содержит ошибки"
                    if attempt < max_retries:
                        print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                        time.sleep(1)  # Небольшая задержка перед повтором
                        continue
                    break
                
                # Файл валиден - переименовываем временный файл в постоянный
                try:
                    temp_file.replace(file_path)
                except (OSError, PermissionError) as e:
                    logger.error(f"Не удалось переместить файл {temp_file} в {file_path}: {e}")
                    last_error = f"Ошибка сохранения файла: {e}"
                    if attempt < max_retries:
                        print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                        time.sleep(1)
                        continue
                    break
                
                # Проверяем, что файл действительно загружен
                if is_log_file_downloaded(file_path):
                    success = True
                    downloaded += 1
                    print(f"{Fore.GREEN}  ✓ Загружен успешно")
                    break
                else:
                    last_error = "Файл не прошел проверку после загрузки"
                    if attempt < max_retries:
                        print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                        time.sleep(1)
                        continue
                    break
                    
            except requests.HTTPError as e:
                # Специальная обработка для 503 Service Unavailable
                if e.response is not None and e.response.status_code == 503:
                    delay = 2 ** attempt  # Экспоненциальная задержка: 2, 4, 8 секунд
                    last_error = f"Ошибка 503 (Service Unavailable)"
                    print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}. Повтор через {delay} сек...")
                    logger.warning(f"Ошибка 503 при загрузке {file_path.name}, попытка {attempt}/{max_retries}")
                    if attempt < max_retries:
                        time.sleep(delay)
                        continue
                    break
                else:
                    status = e.response.status_code if e.response is not None else "unknown"
                    last_error = f"Ошибка HTTP {status}: {e}"
                    if attempt < max_retries:
                        print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                        time.sleep(1)
                        continue
                    break
            except requests.RequestException as e:
                last_error = f"Ошибка сети: {e}"
                if attempt < max_retries:
                    print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                    time.sleep(1)
                    continue
                break
            except (OSError, PermissionError) as e:
                last_error = f"Ошибка файловой системы: {e}"
                if attempt < max_retries:
                    print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                    time.sleep(1)
                    continue
                break
            except Exception as e:
                last_error = f"Неожиданная ошибка: {e}"
                if attempt < max_retries:
                    print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                    time.sleep(1)
                    continue
                break
        
        if not success:
            failed += 1
            if last_error:
                if str(last_error).startswith("Ошибка базы данных:"):
                    db_missing += 1
                print(f"{Fore.RED}  ✗ Не удалось загрузить после {max_retries} попыток: {last_error}")
                logger.error(f"Не удалось загрузить {file_path.name} после {max_retries} попыток: {last_error}")
        
        # Обновляем прогресс-бар
        current_progress += 1
        if total_files > 0:
            update_progress_bar(current_progress, total_files)
    
    # Завершаем прогресс-бар
    if total_files > 0:
        print()  # Переход на новую строку после прогресс-бара
    
    downloaded += existing_files_count
    
    return downloaded, failed, db_missing


# Определяет тип диапазона (L или X) по количеству столбцов в заголовке лог-файла
def detect_bend_type_from_header(log_file_path: Path) -> str:
    """
    Определяет тип диапазона (L или X) из заголовка лог-файла.
    
    Args:
        log_file_path: Путь к лог-файлу
        
    Returns:
        str: "L" или "X"
    """
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#Time'):
                    # Анализируем заголовок
                    # L bend: #Time	Az	El	Level	SNR (5 столбцов до SNR)
                    # X bend: #Time	Az	El	Level	Level2	SNR (6 столбцов до SNR)
                    parts = line.split('\t')
                    if 'Level2' in line or len(parts) > 6:
                        return "X"
                    else:
                        return "L"
        # По умолчанию L, если не удалось определить
        return "L"
    except (FileNotFoundError, PermissionError, IOError, OSError) as e:
        logger.debug(f"Не удалось определить тип диапазона из файла {log_file_path}: {e}")
        return "L"


# Извлекает все значения SNR из лог-файла и возвращает сумму SNR и количество измерений
def extract_snr_from_log(log_file_path: Path, bend_type: Optional[str] = None) -> Tuple[float, int]:
    """
    Извлекает значения SNR из лог-файла и возвращает сумму SNR и количество измерений.
    Оптимизировано для минимизации потребления памяти.
    
    Args:
        log_file_path: Путь к лог-файлу
        bend_type: Тип диапазона ("L" или "X"). Если None, определяется автоматически из заголовка
        
    Returns:
        Tuple[float, int]: (сумма SNR, количество измерений)
    """
    snr_sum = 0.0
    snr_count = 0
    
    # Определяем тип диапазона, если не указан
    if bend_type is None:
        bend_type = detect_bend_type_from_header(log_file_path)
    
    # Определяем индекс столбца SNR в зависимости от типа диапазона
    # L bend: Time, Az, El, Level, SNR -> индекс 4
    # X bend: Time, Az, El, Level, Level2, SNR -> индекс 5
    snr_column_index = 5 if bend_type == "X" else 4
    
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Пропускаем комментарии и пустые строки без лишних операций
                if not line.strip() or line.startswith('#'):
                    continue
                
                # Разбиваем строку по табуляции
                parts = line.split('\t')
                
                if len(parts) > snr_column_index:
                    try:
                        snr_sum += float(parts[snr_column_index])
                        snr_count += 1
                    except (ValueError, IndexError):
                        continue
        
        return snr_sum, snr_count
            
    except (FileNotFoundError, PermissionError) as e:
        logger.error(f"Не удалось прочитать файл {log_file_path}: {e}")
        print_error(f"Ошибка доступа к файлу {log_file_path}: {e}", is_critical=False)
        return 0.0, 0
    except (IOError, OSError) as e:
        logger.error(f"Ошибка ввода-вывода при чтении файла {log_file_path}: {e}")
        print_error(f"Ошибка чтения файла {log_file_path}: {e}", is_critical=False)
        return 0.0, 0
    except Exception as e:
        logger.error(f"Неожиданная ошибка при чтении файла {log_file_path}: {e}", exc_info=True)
        print_error(f"Неожиданная ошибка при чтении файла {log_file_path}: {e}", is_critical=False)
        return 0.0, 0


# Рассчитывает статистику SNR для всех логов станции и выводит пустые пролеты
def calculate_avg_snr_for_station(station_folder: Path, bend_type: Optional[str] = None, show_only_unsuccessful: bool = True) -> List[Tuple[str, float, int]]:
    """
    Рассчитывает сумму SNR для всех лог-файлов в папке станции.
    
    Args:
        station_folder: Путь к папке станции
        bend_type: Тип диапазона ("L" или "X"). Если None, определяется автоматически
        show_only_unsuccessful: Если True, выводит только пустые пролеты
        
    Returns:
        List[Tuple[str, float, int]]: Список кортежей (имя файла, сумма SNR, количество измерений)
    """
    results = []
    
    if not station_folder.exists() or not station_folder.is_dir():
        print(f"{Fore.YELLOW}Папка {station_folder} не существует или не является директорией", file=sys.stderr)
        return results
    
    # Находим все .log файлы
    log_files = sorted(station_folder.glob("*.log"))
    
    if not log_files:
        print(f"{Fore.YELLOW}В папке {station_folder} не найдено лог-файлов", file=sys.stderr)
        return results
    
    print(f"\n{Fore.CYAN}Обработка станции: {station_folder.name}")
    print(f"{Fore.CYAN}Найдено лог-файлов: {len(log_files)}\n")
    
    # Определяем порог "пустоты" пролета в зависимости от типа диапазона
    bend_type_upper = (bend_type or "L").upper()
    threshold = X_BEND_FAILURE_THRESHOLD if bend_type_upper == "X" else L_BEND_FAILURE_THRESHOLD
    
    # Собираем все результаты
    for log_file in log_files:
        snr_sum, count = extract_snr_from_log(log_file, bend_type)
        results.append((log_file.name, snr_sum, count))
    
    # Фильтруем пустые пролеты, если требуется
    if show_only_unsuccessful:
        unsuccessful_results = []
        for filename, snr_sum, count in results:
            avg_snr = snr_sum / count if count > 0 else 0
            if avg_snr <= threshold:
                unsuccessful_results.append((filename, snr_sum, count))
        
        if unsuccessful_results:
            # Сортируем "пустые" результаты по сумме SNR (по возрастанию - от меньшего к большему)
            unsuccessful_results.sort(key=lambda x: x[1])
            
            # Выводим только пустые пролеты
            print(f"{Fore.RED}{'Пустые пролеты (средний SNR <= ' + str(threshold) + ')':<60}")
            print(f"{Fore.CYAN}{'Имя файла':<60} {'Сумма SNR':>12} {'Средний SNR':>15}")
            print("-" * 90)
            for filename, snr_sum, count in unsuccessful_results:
                avg_snr = snr_sum / count if count > 0 else 0
                print(f"{Fore.RED}{filename:<60} {snr_sum:>12.2f} {avg_snr:>15.2f}")
        else:
            print(f"{Fore.GREEN}Пустых пролетов не найдено")
    else:
        # Выводим все результаты (старое поведение)
        results.sort(key=lambda x: x[1], reverse=True)
        print(f"{Fore.CYAN}{'Имя файла':<60} {'Сумма SNR':>12}")
        print("-" * 75)
        for filename, snr_sum, count in results:
            print(f"{Fore.CYAN}{filename:<60} {snr_sum:>12.2f}")
    
    return results


# Анализирует логи выбранных спутников по станции за диапазон дат
def analyze_satellite_logs_for_station(
    station_name: str,
    start_date: datetime,
    end_date: datetime,
    satellites: Optional[List[str]] = None,
) -> None:
    """
    Анализирует все логи станции за период и фильтрует только по указанным спутникам.
    Сат эл. определяется по вхождению строки в имя файла.
    """
    stations, station_bend_map = load_stations_from_config_for_analysis()
    bend_type = station_bend_map.get(station_name)
    bend_type_upper = (bend_type or "L").upper()
    threshold = X_BEND_FAILURE_THRESHOLD if bend_type_upper == "X" else L_BEND_FAILURE_THRESHOLD

    sat_tokens = [s.upper() for s in satellites] if satellites else []

    current_date = start_date
    total_files = 0
    successful = 0
    unsuccessful = 0
    total_measurements = 0
    total_snr_sum = 0.0

    if satellites:
        print(f"{Fore.CYAN + Style.BRIGHT}\nАНАЛИЗ ПО СПУТНИКАМ (станция: {station_name})")
        print(f"{Fore.CYAN}Спутники: {', '.join(satellites)}")
    else:
        print(f"{Fore.CYAN + Style.BRIGHT}\nАНАЛИЗ ПО ВСЕМ ПРОЛЕТАМ (станция: {station_name})")
    print(f"{Fore.CYAN}Период: {start_date.strftime('%Y%m%d')} — {end_date.strftime('%Y%m%d')}")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        year, month, _, _ = get_date_paths(date_str)
    base_logs_dir = Path("/root/lorett/GroundLinkServer/logs") / year / month / date_str / station_name

        if not base_logs_dir.exists():
            print(f"{Fore.YELLOW}  {date_str}: папка не найдена, пропуск")
            current_date += timedelta(days=1)
            continue

        log_files = sorted(base_logs_dir.glob("*.log"))
        if not log_files:
            print(f"{Fore.YELLOW}  {date_str}: лог-файлы не найдены")
            current_date += timedelta(days=1)
            continue

        date_count = 0
        date_success = 0
        date_fail = 0
        date_snr_sum = 0.0
        date_measurements = 0
        date_files: List[Tuple[str, bool]] = []

        for log_file in log_files:
            name_upper = log_file.name.upper()
            if sat_tokens and not any(tok in name_upper for tok in sat_tokens):
                continue
            snr_sum, count = extract_snr_from_log(log_file, bend_type)
            avg_snr = snr_sum / count if count > 0 else 0.0

            date_count += 1
            date_files.append((log_file.name, avg_snr > threshold))
            date_measurements += count
            date_snr_sum += snr_sum
            if avg_snr > threshold:
                date_success += 1
            else:
                date_fail += 1

        if date_count > 0:
            avg_snr_date = (date_snr_sum / date_measurements) if date_measurements > 0 else 0.0
            print(
                f"{Fore.BLUE}  {date_str}: файлов {date_count}, успешных {date_success}, "
                f"пустых {date_fail}, средний SNR {avg_snr_date:.2f}"
            )
            print(f"{Fore.CYAN}    Пролеты:")
            for fname, is_success in sorted(date_files):
                color = Fore.GREEN if is_success else Fore.RED
                print(f"{color}      - {fname}")
            print()
            total_files += date_count
            successful += date_success
            unsuccessful += date_fail
            total_measurements += date_measurements
            total_snr_sum += date_snr_sum
        else:
            print(f"{Fore.YELLOW}  {date_str}: подходящих логов нет")
            print()

        current_date += timedelta(days=1)

    overall_avg = (total_snr_sum / total_measurements) if total_measurements > 0 else 0.0
    print(f"{Fore.CYAN + Style.BRIGHT}\nИТОГО")
    print(f"{Fore.CYAN}Файлов: {total_files}, Успешных: {successful}, Пустых: {unsuccessful}, Средний SNR: {overall_avg:.2f}")

# Получает график пролета через браузер (Playwright/Pyppeteer) и сохраняет как PNG
async def get_log_graph(station_name: str, log_filename: str, output_dir: Path):
    """
    Получает график пролета из веб-интерфейса и сохраняет как изображение.
    
    Args:
        station_name: Имя станции
        log_filename: Имя файла лога
        output_dir: Директория для сохранения изображения
    """
    # Подготавливаем путь к изображению
    output_dir.mkdir(parents=True, exist_ok=True)
    image_name = log_filename.replace('.log', '.png').replace(' ', '_')
    image_path = output_dir / image_name
    
    # Если изображение уже существует, пропускаем загрузку
    if image_path.exists():
        logger.debug(f"График уже существует, пропускаем: {image_path}")
        return image_path
    
    try:
        # Пробуем сначала playwright (более современный и надежный)
        try:
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                # Используем chromium с headless режимом
                browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
                page = await browser.new_page()
                url = f'http://eus.lorett.org/eus/log_view/{log_filename}'
                await page.goto(url, wait_until='networkidle', timeout=30000)
                await page.set_viewport_size({'width': GRAPH_VIEWPORT_WIDTH, 'height': GRAPH_VIEWPORT_HEIGHT})
                await asyncio.sleep(GRAPH_LOAD_DELAY)
                # Прокручиваем к начальной точке графика, если задано смещение
                if GRAPH_SCROLL_X > 0 or GRAPH_SCROLL_Y > 0:
                    await page.evaluate(f"window.scrollTo({GRAPH_SCROLL_X}, {GRAPH_SCROLL_Y})")
                    await asyncio.sleep(0.2)  # Небольшая задержка после скролла
                await page.screenshot(path=str(image_path), full_page=False)
                await browser.close()
            
            return image_path
        except ImportError:
            # Если playwright не установлен, пробуем pyppeteer
            from pyppeteer import launch
            import os
            import sys
            
            # Проверка уже выполнена в начале функции, но на всякий случай проверяем еще раз
            if image_path.exists():
                return image_path
            
            # Отключаем автоматическую загрузку Chromium
            os.environ['PYPPETEER_SKIP_CHROMIUM_DOWNLOAD'] = '1'
            
            # Пытаемся найти установленный Chrome/Chromium
            chrome_paths = [
                str(BASE_DIR / "bin" / "chrome"),
                str(BASE_DIR / "bin" / "chromium"),
                str(BASE_DIR / "chrome" / "chrome"),
            ]
            
            executable_path = None
            for path in chrome_paths:
                if os.path.exists(path):
                    executable_path = path
                    break
            
            if not executable_path:
                raise Exception("Chrome/Chromium не найден. Установите Chrome или используйте: pip install playwright && playwright install chromium")
            
            browser = await launch({
                'executablePath': executable_path,
                'args': ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
            })
            page = await browser.newPage()
            url = f'http://eus.lorett.org/eus/log_view/{log_filename}'
            await page.goto(url, waitUntil='networkidle0', timeout=30000)
            await page.setViewport({'width': GRAPH_VIEWPORT_WIDTH, 'height': GRAPH_VIEWPORT_HEIGHT})
            await asyncio.sleep(GRAPH_LOAD_DELAY)
            # Прокручиваем к начальной точке графика, если задано смещение
            if GRAPH_SCROLL_X > 0 or GRAPH_SCROLL_Y > 0:
                await page.evaluate(f"window.scrollTo({GRAPH_SCROLL_X}, {GRAPH_SCROLL_Y})")
                await asyncio.sleep(0.2)  # Небольшая задержка после скролла
            await page.screenshot({'path': str(image_path), 'fullPage': False})
            await browser.close()
            
            return image_path
    except ImportError:
        logger.warning("playwright или pyppeteer не установлены")
        print(f"{Fore.YELLOW}Предупреждение: playwright или pyppeteer не установлены. Установите: pip install playwright && playwright install chromium")
        return None
    except (asyncio.TimeoutError, RuntimeError) as e:
        logger.error(f"Ошибка при получении графика для {log_filename}: {e}")
        print_error(f"Ошибка при получении графика для {log_filename}: {e}", is_critical=False)
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении графика для {log_filename}: {e}", exc_info=True)
        print_error(f"Ошибка при получении графика для {log_filename}: {e}", is_critical=False)
        return None


# Главная функция анализа: обрабатывает логи, считает статистику SNR, загружает графики и генерирует отчет
def analyze_downloaded_logs(target_date: str) -> None:
    """
    Анализирует все скачанные лог-файлы за указанную дату.
    
    Рассчитывает сумму SNR для каждой станции, фильтрует пролеты по порогу SNR,
    определяет успешные/пустые пролеты и загружает графики для файлов
    с максимальной суммой SNR.
    
    Args:
        target_date: Дата в формате YYYYMMDD для анализа логов
    """
    # Используем папку для логов в формате logs\YYYY\MM\YYYYMMDD
    year, month, date_str, _ = get_date_paths(target_date)
    logs_dir = Path("/root/lorett/GroundLinkServer/logs")
    base_logs_dir = logs_dir / year / month / date_str
    
    if not base_logs_dir.exists():
        print(f"{Fore.YELLOW}Папка {base_logs_dir} не существует, анализ пропущен")
        return
    
    logger.info(f"Начало анализа SNR для даты {target_date}")
    print(f"{Fore.CYAN + Style.BRIGHT}\nАНАЛИЗ SNR")
    print(f"{Fore.BLUE}Обработка логов за дату: {target_date}")
    
    # Загружаем конфигурацию для получения типов диапазонов
    try:
        stations, station_bend_map = load_stations_from_config_for_analysis()
        config_path = Path('/root/lorett/GroundLinkServer/config.json')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        else:
            config = {}
    except SystemExit:
        stations = {}
        station_bend_map = {}
        config = {}
        print(f"{Fore.YELLOW}Предупреждение: не удалось загрузить конфигурацию, анализ будет без учета типов диапазонов")
    
    all_results = {}
    
    # Обрабатываем все станции из конфига
    for station_name in stations.keys():
        station_folder = base_logs_dir / station_name
        
        bend_type = station_bend_map.get(station_name)
        bend_type_upper = (bend_type or "L").upper()
        
        # Инициализируем запись для всех станций из конфига
        all_results[station_name] = {
            'files': 0,
            'avg_snr': 0.0,
            'measurements': 0,
            'max_snr_filename': '',
            'max_snr_value': 0.0,
            'successful_passes': 0,
            'unsuccessful_passes': 0,
            'unsuccessful_filenames': [],  # Список пустых пролетов
            'bend_type': bend_type_upper,
            'results': []
        }
        
        if not station_folder.exists():
            continue
        
        if bend_type:
            print(f"\n{Fore.CYAN}Обработка станции {station_name} (диапазон: {bend_type})")
        else:
            print(f"\n{Fore.CYAN}Обработка станции {station_name}")
        
        # Выводим только пустые пролеты
        results = calculate_avg_snr_for_station(station_folder, bend_type, show_only_unsuccessful=True)
        
        if results:
            # Для станции R2.0S_Moscow исключаем пролеты TY из подсчета
            if station_name == "R2.0S_Moscow":
                original_count = len(results)
                results = [(filename, snr_sum, count) for filename, snr_sum, count in results if "TY" not in filename]
                filtered_count = original_count - len(results)
                if filtered_count > 0:
                    logger.info(f"Станция {station_name}: исключено {filtered_count} пролетов TY из подсчета")
                if not results:
                    logger.info(f"Станция {station_name}: все пролеты были отфильтрованы (TY пролеты исключены)")
                    continue
            
            # Фильтруем пролеты: оставляем только те, где средний SNR > MIN_AVG_SNR_THRESHOLD
            filtered_results = []
            for filename, snr_sum, count in results:
                avg_snr = snr_sum / count if count > 0 else 0
                if avg_snr > MIN_AVG_SNR_THRESHOLD:
                    filtered_results.append((filename, snr_sum, count))
            
            # Вычисляем общую статистику
            total_files = len(results)
            
            # Для расчета суммы SNR используем только отфильтрованные результаты (средний SNR > 4)
            if filtered_results:
                # Сортируем отфильтрованные результаты по сумме SNR (по убыванию)
                filtered_results.sort(key=lambda x: x[1], reverse=True)
                # Вычисляем средний SNR для станции из отфильтрованных результатов
                total_sum = sum(snr_sum for _, snr_sum, _ in filtered_results)
                total_measurements = sum(count for _, _, count in filtered_results)
                total_avg = total_sum / total_measurements if total_measurements > 0 else 0
                # Первый в отсортированном списке - с максимальной суммой SNR
                max_snr_filename, max_snr_value, _ = filtered_results[0]
            else:
                # Если нет пролетов со средним SNR > 4, используем все результаты
                total_sum = sum(snr_sum for _, snr_sum, _ in results)
                total_measurements = sum(count for _, _, count in results)
                total_avg = total_sum / total_measurements if total_measurements > 0 else 0
                # Результаты уже отсортированы по сумме SNR в порядке убывания
                max_snr_filename, max_snr_value, _ = results[0]
            
            # Подсчет успешных и пустых пролетов
            successful_passes = 0
            unsuccessful_passes = 0
            unsuccessful_filenames = []  # Список имен пустых пролетов
            successful_results = []  # (filename, snr_sum, count) только успешные пролеты
            
            # Определяем порог успешности в зависимости от диапазона
            threshold = X_BEND_FAILURE_THRESHOLD if bend_type_upper == "X" else L_BEND_FAILURE_THRESHOLD
            
            for filename, snr_sum, count in results:
                # Вычисляем среднее SNR для определения успешности пролета
                avg_snr = snr_sum / count if count > 0 else 0
                if avg_snr > threshold:
                    successful_passes += 1
                    successful_results.append((filename, snr_sum, count))
                else:
                    unsuccessful_passes += 1
                    unsuccessful_filenames.append(filename)

            # Если есть хотя бы один успешный пролет — выбираем "макс. сумму SNR" среди успешных,
            # чтобы график строился по реально успешному лог-файлу.
            if successful_results:
                successful_results.sort(key=lambda x: x[1], reverse=True)
                max_snr_filename, max_snr_value, _ = successful_results[0]
            
            all_results[station_name] = {
                'files': total_files,
                'avg_snr': total_avg,
                'measurements': total_measurements,
                'max_snr_filename': max_snr_filename,
                'max_snr_value': max_snr_value,
                'successful_passes': successful_passes,
                'unsuccessful_passes': unsuccessful_passes,
                'unsuccessful_filenames': unsuccessful_filenames,  # Список пустых пролетов
                'bend_type': bend_type_upper,
                'results': results
            }
            
            logger.info(f"Станция {station_name}: файлов {total_files}, средний SNR {total_avg:.2f}, успешных {successful_passes}, пустых {unsuccessful_passes}")
            print(f"{Fore.GREEN}  Всего файлов: {total_files}, Средний SNR: {total_avg:.2f}")
            print(f"{Fore.GREEN}  Успешных пролетов: {successful_passes}, Пустых пролетов: {unsuccessful_passes}")
            
            # Сохраняем результаты в файл
            output_file = station_folder / f"avg_snr_{station_folder.name}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"Сумма SNR для станции: {station_folder.name}\n")
                f.write("\n")
                f.write(f"{'Имя файла':<60} {'Сумма SNR':>12}\n")
                f.write("-" * 75 + "\n")
                for filename, snr_sum, count in results:
                    f.write(f"{filename:<60} {snr_sum:>12.2f}\n")
                f.write("\n")
                f.write(f"Всего файлов обработано: {total_files}\n")
                f.write(f"Средний SNR по всем измерениям: {total_avg:.2f}\n")
                f.write(f"Успешных пролетов: {successful_passes}\n")
                f.write(f"Пустых пролетов: {unsuccessful_passes}\n")
            
            print(f"{Fore.GREEN}  Результаты сохранены в файл: {output_file}")
    
    # Перед итоговой сводкой обновляем all_passes.db по станциям
    try:
        update_all_passes_db_for_date(target_date)
        print_comm_passes_status(target_date)
    except Exception as e:
        logger.error(f"Не удалось обновить all_passes.db: {e}", exc_info=True)

    # Итоговая сводка
    if stations:
        # Преобразуем дату из YYYYMMDD в YYYY-MM-DD для отображения
        date_display = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
        print(f"\n{Fore.CYAN + Style.BRIGHT}\nИТОГОВАЯ СВОДКА ПО ВСЕМ СТАНЦИЯМ  {date_display}")
        print(f"{Fore.CYAN}{'Станция':<30} {'Всего':>10} {'Успешных':>12} {'Пустых':>14} {'% пустых':>15} {'Средний SNR':>15}")
        print("-" * 115)
        
        # Сортируем станции по среднему SNR
        sorted_stations = sorted(all_results.items(), key=lambda x: x[1]['avg_snr'], reverse=True)
        
        for station_name, stats in sorted_stations:
            unsuccessful_percent = (stats['unsuccessful_passes'] / stats['files'] * 100) if stats['files'] > 0 else 0.0
            print(f"{Fore.CYAN}{station_name:<30} {stats['files']:>10} {stats['successful_passes']:>12} {stats['unsuccessful_passes']:>14} {unsuccessful_percent:>14.1f}% {stats['avg_snr']:>15.2f}")
        
        total_all_files = sum(stats['files'] for stats in all_results.values())
        total_successful = sum(stats['successful_passes'] for stats in all_results.values())
        total_unsuccessful = sum(stats['unsuccessful_passes'] for stats in all_results.values())
        total_unsuccessful_percent = (total_unsuccessful / total_all_files * 100) if total_all_files > 0 else 0.0
        # Вычисляем общий средний SNR: сумма всех SNR / количество всех измерений
        total_all_measurements = sum(stats['measurements'] for stats in all_results.values())
        total_all_snr_sum = sum(stats['avg_snr'] * stats['measurements'] for stats in all_results.values())
        overall_avg = total_all_snr_sum / total_all_measurements if total_all_measurements > 0 else 0
        
        print("-" * 115)
        print(f"{Fore.GREEN + Style.BRIGHT}{'ИТОГО':<30} {total_all_files:>10} {total_successful:>12} {total_unsuccessful:>14} {total_unsuccessful_percent:>14.1f}% {overall_avg:>15.2f}")
        
        # Список файлов с максимальной суммой SNR
        print(f"\n{Fore.CYAN + Style.BRIGHT}\nФАЙЛЫ С МАКСИМАЛЬНОЙ СУММОЙ SNR ПО СТАНЦИЯМ")
        print(f"{Fore.CYAN}{'Станция':<30} {'Файл с макс. суммой SNR':<80} {'Сумма SNR':>15}")
        print("-" * 140)
        
        # Создаем папку для графиков в формате report\YYYY\MM\DD.MM.YYYY
        # ВАЖНО: не удаляем существующую директорию, чтобы не скачивать уже сохраненные графики повторно.
        year, month, _, date_folder = get_date_paths(target_date)
        graphs_dir = Path('/root/lorett/GroundLinkServer/report') / year / month / date_folder
        try:
            graphs_dir.mkdir(parents=True, exist_ok=True)
        except (PermissionError, OSError) as e:
            logger.error(f"Не удалось создать директорию {graphs_dir}: {e}")
            print_error(f"Не удалось создать директорию для графиков: {e}", is_critical=True)
            return
        
        # Собираем задачи для получения графиков
        graph_tasks = []
        
        for station_name, stats in sorted_stations:
            if stats['files'] == 0:
                print(f"{Fore.YELLOW}{station_name:<30} {'нет данных':<80} {'':>15}")
            else:
                # Станцию считаем "не работает" только если нет ни одного успешного пролета
                # (avg_snr по станции может быть низким из-за большого количества пустых пролетов).
                if stats.get('successful_passes', 0) <= 0 or not stats.get('max_snr_filename'):
                    print(f"{Fore.RED}{station_name:<30} {'станция не работает':<80} {'':>15}")
                else:
                    print(f"{Fore.CYAN}{station_name:<30} {stats['max_snr_filename']:<80} {stats['max_snr_value']:>15.2f}")
                    # Добавляем задачу для получения графика
                    graph_tasks.append((station_name, stats['max_snr_filename']))
        
        # Получаем графики для файлов с максимальной суммой SNR
        if graph_tasks:
            print(f"\n{Fore.CYAN + Style.BRIGHT}\nЗАГРУЗКА ГРАФИКОВ ПРОЛЕТОВ С МАКСИМАЛЬНОЙ СУММОЙ SNR")
            async def download_all_graphs():
                tasks = []
                for station_name, log_filename in graph_tasks:
                    print(f"{Fore.BLUE}Загрузка графика: {station_name} - {log_filename}")
                    task = get_log_graph(station_name, log_filename, graphs_dir)
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, (station_name, log_filename) in enumerate(graph_tasks):
                    if isinstance(results[i], Exception):
                        print(f"{Fore.RED}Ошибка при загрузке графика для {log_filename}: {results[i]}")
                    elif results[i]:
                        print(f"{Fore.GREEN}График сохранен: {results[i]}")
            
            try:
                # Исправление проблемы с asyncio.get_event_loop()
                # Проверяем, есть ли уже запущенный event loop
                try:
                    # Пытаемся получить текущий running loop
                    asyncio.get_running_loop()
                    # Если дошли сюда, значит loop уже запущен - используем create_task
                    # Но так как мы в синхронной функции, лучше создать новый loop
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(download_all_graphs())
                    loop.close()
                except RuntimeError:
                    # Если loop не запущен, используем asyncio.run() (Python 3.7+)
                    try:
                        asyncio.run(download_all_graphs())
                    except AttributeError:
                        # Для старых версий Python (< 3.7) используем get_event_loop()
                        loop = asyncio.get_event_loop()
                        if loop.is_closed():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                        loop.run_until_complete(download_all_graphs())
            except (RuntimeError, asyncio.TimeoutError) as e:
                logger.error(f"Ошибка при загрузке графиков: {e}", exc_info=True)
                print_error(f"Ошибка при загрузке графиков: {e}", is_critical=False)
            except Exception as e:
                logger.error(f"Неожиданная ошибка при загрузке графиков: {e}", exc_info=True)
                print_error(f"Неожиданная ошибка при загрузке графиков: {e}", is_critical=False)
        
        # Отправляем статистику на почту (настройки по умолчанию берём из test_email.py,
        # но можно переопределить через config.json -> email.* или переменные окружения)
        try:
            email_settings = get_email_settings(config)
            if EMAIL_DISABLED:
                logger.info("Email отключен флагом -OffEmail, отправка пропущена")
            elif email_settings.get("enabled"):
                subject = "Сводка работы станций"
                # Генерируем сводный график общего % пустых за 7 дней
                summary_chart_path = graphs_dir / "overall_unsuccessful_7d.png"
                generated_summary = generate_overall_unsuccessful_7d_chart(
                    target_date=target_date,
                    stations=stations,
                    station_bend_map=station_bend_map,
                    output_path=summary_chart_path,
                    days=7,
                )
                comm_summary_chart_path = graphs_dir / "comm_unsuccessful_7d.png"
                generated_comm_summary = generate_comm_unsuccessful_7d_chart(
                    target_date=target_date,
                    output_path=comm_summary_chart_path,
                    days=7,
                )
                comm_stats, comm_totals = _comm_collect_stats(target_date)
                comm_links = _comm_collect_log_links(target_date)

                body, inline_images = build_stats_email_body(
                    target_date,
                    all_results,
                    graphs_dir,
                    generated_summary,
                    comm_stats,
                    comm_totals,
                    generated_comm_summary,
                    comm_links,
                )
                attachments = []

                ok = send_stats_email(
                    smtp_server=email_settings["smtp_server"],
                    smtp_port=int(email_settings["smtp_port"]),
                    sender_email=email_settings["sender_email"],
                    sender_password=email_settings["sender_password"],
                    recipients=email_settings["recipients"],
                    cc_recipients=email_settings.get("cc_recipients") or [],
                    subject=subject,
                    body=body,
                    attachments=attachments,
                    inline_images=inline_images,
                )
                if ok:
                    print(f"{Fore.GREEN}Статистика отправлена на почту: {', '.join(email_settings['recipients'])}")
                else:
                    print(f"{Fore.YELLOW}Предупреждение: не удалось отправить статистику на почту (см. лог)")
        except Exception as e:
            logger.warning(f"Неожиданная ошибка при отправке email: {e}", exc_info=True)
            print(f"{Fore.YELLOW}Предупреждение: не удалось отправить email: {e}")

# Загружает конфигурацию станций и создает словари: станция->тип и станция->диапазон (bend)
def load_stations_from_config_for_analysis(config_path: Path = Path("/root/lorett/GroundLinkServer/config.json")) -> Tuple[dict, dict]:
    """
    Загружает список станций из config.json и создает словарь соответствия станция -> bend тип.
    Используется для анализа SNR.
    
    Args:
        config_path: Путь к файлу конфигурации
        
    Returns:
        Tuple[dict, dict]: (словарь {имя_станции: тип_станции}, словарь {имя_станции: bend_тип})
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            stations_dict = {}
            station_bend_map = {}
            
            for station in config.get('stations', []):
                station_name = station['name']
                # type (oper/reg) больше не нужен для анализа, сохраняем заглушку
                stations_dict[station_name] = "station"
                # Используем "bend" или "range" (для обратной совместимости)
                bend_type = station.get('bend') or station.get('range')
                if bend_type:
                    station_bend_map[station_name] = bend_type.upper()
            
            return stations_dict, station_bend_map
    except FileNotFoundError:
        print(f"{Fore.RED}Ошибка: файл конфигурации {config_path} не найден", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"{Fore.RED}Ошибка при чтении конфигурации: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyError as e:
        print(f"{Fore.RED}Ошибка в структуре конфигурации: {e}", file=sys.stderr)
        sys.exit(1)


def run_daily_report():
    """
    Запускает обработку и отправку статистики за вчерашний день.
    Вызывается автоматически в 00:00 UTC.
    """
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("НАЧАЛО АВТОМАТИЧЕСКОЙ ОТПРАВКИ СТАТИСТИКИ")
    logger.info("=" * 60)
    
    try:
        # Получаем вчерашнюю дату в UTC
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime("%Y%m%d")
        date_display = yesterday.strftime("%d.%m.%Y")
        
        logger.info(f"Дата для отчёта: {date_display} ({date_str})")
        logger.info(f"Время запуска: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        _log_config_full()
        print(f"{Fore.CYAN + Style.BRIGHT}\n{'='*80}")
        print(f"{Fore.CYAN + Style.BRIGHT}АВТОМАТИЧЕСКАЯ ОТПРАВКА СТАТИСТИКИ")
        print(f"{Fore.CYAN}Дата: {date_display} ({date_str})")
        print(f"{Fore.CYAN + Style.BRIGHT}{'='*80}\n")
        
        # Сначала скачиваем логи
        logger.info("Этап 1/2: Скачивание логов...")
        download_start = datetime.now(timezone.utc)
        download_logs_for_date(date_str)
        download_duration = (datetime.now(timezone.utc) - download_start).total_seconds()
        logger.info(f"Этап 1/2: Скачивание логов завершено за {download_duration:.1f} сек")
        
        # Затем анализируем и отправляем статистику
        logger.info("Этап 2/2: Анализ логов и отправка email...")
        analyze_start = datetime.now(timezone.utc)
        analyze_downloaded_logs(date_str)
        analyze_duration = (datetime.now(timezone.utc) - analyze_start).total_seconds()
        logger.info(f"Этап 2/2: Анализ и отправка завершены за {analyze_duration:.1f} сек")
        
        total_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"Автоматическая отправка статистики за {date_str} завершена успешно")
        logger.info(f"Общее время выполнения: {total_duration:.1f} сек")
        logger.info("=" * 60)
        print(f"{Fore.GREEN}Автоматическая отправка статистики за {date_display} завершена")
    except Exception as e:
        total_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.error(f"ОШИБКА при автоматической отправке статистики: {e}")
        logger.error(f"Время до ошибки: {total_duration:.1f} сек")
        logger.error("Traceback:", exc_info=True)
        logger.info("=" * 60)
        print(f"{Fore.RED}Ошибка при автоматической отправке статистики: {e}")


def scheduler_loop():
    """
    Планировщик, который запускает отправку статистики в 00:00 UTC каждый день.
    """
    logger.info("=" * 60)
    logger.info("ПЛАНИРОВЩИК LORETT GROUND LINK MONITOR ЗАПУЩЕН")
    logger.info("=" * 60)
    logger.info(f"Время запуска: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    _log_config_full()
    logger.info("Режим: ежедневная отправка статистики в 00:00 UTC")
    print(f"{Fore.GREEN}Планировщик автоматической отправки статистики запущен")
    print(f"{Fore.CYAN}Отправка будет происходить каждый день в 00:00 UTC")
    print(f"{Fore.CYAN}Для остановки нажмите Ctrl+C\n")
    
    report_count = 0
    error_count = 0
    
    while True:
        try:
            # Получаем текущее время в UTC
            now_utc = datetime.now(timezone.utc)
            
            # Вычисляем время до следующей полуночи UTC
            next_midnight_utc = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            # ВАЖНО: "полночь" — это ровно 00:00:00, а не вся минута 00:00.
            # Иначе при 00:00:17 мы получаем next_midnight в прошлом (00:00:00),
            # отрицательное wait_seconds и повторный запуск отчёта за тот же день.
            if now_utc.hour == 0 and now_utc.minute == 0 and now_utc.second == 0:
                # Если уже ровно 00:00:00, запускаем сразу
                next_midnight_utc = now_utc.replace(microsecond=0)
            
            wait_seconds = (next_midnight_utc - now_utc).total_seconds()
            if wait_seconds < 0:
                # На всякий случай: если next_midnight оказался в прошлом, переносим на следующий день
                next_midnight_utc = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                wait_seconds = (next_midnight_utc - now_utc).total_seconds()
            
            # Если до полуночи меньше минуты, запускаем сразу
            if wait_seconds < 60:
                logger.info("До полуночи UTC меньше минуты, запускаем отправку немедленно")
                report_count += 1
                logger.info(f"Отчёт #{report_count} начинается...")
                run_daily_report()
                logger.info(f"Отчёт #{report_count} завершён")
                # После отправки ждём до следующей полуночи
                next_midnight_utc = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                wait_seconds = (next_midnight_utc - datetime.now(timezone.utc)).total_seconds()
            
            wait_hours = wait_seconds / 3600
            wait_minutes = (wait_seconds % 3600) / 60
            next_time_str = next_midnight_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
            
            logger.info(f"Ожидание следующей отправки: {next_time_str}")
            logger.info(f"Время ожидания: {int(wait_hours)} ч {int(wait_minutes)} мин")
            logger.info(f"Статистика: отчётов отправлено: {report_count}, ошибок: {error_count}")
            print(f"{Fore.CYAN}Следующая отправка: {next_time_str} (через {wait_hours:.1f} часов)")
            
            # Ждём до следующей полуночи
            time.sleep(wait_seconds)
            
            # Запускаем отправку
            logger.info("Время отправки наступило!")
            report_count += 1
            logger.info(f"Отчёт #{report_count} начинается...")
            run_daily_report()
            logger.info(f"Отчёт #{report_count} завершён")
            
        except KeyboardInterrupt:
            logger.info("=" * 60)
            logger.info("ПЛАНИРОВЩИК ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)")
            logger.info(f"Итого отправлено отчётов: {report_count}")
            logger.info(f"Итого ошибок: {error_count}")
            logger.info("=" * 60)
            print(f"\n{Fore.YELLOW}Планировщик остановлен")
            break
        except Exception as e:
            error_count += 1
            logger.error(f"ОШИБКА #{error_count} в планировщике: {e}")
            logger.error("Traceback:", exc_info=True)
            logger.info(f"Повторная попытка через 60 секунд...")
            print(f"{Fore.RED}Ошибка в планировщике: {e}")
            # Ждём минуту перед повтором
            time.sleep(60)


if __name__ == "__main__":
    import sys
    _run_comm_passes_sync()
    # Флаг: отключить отправку email
    if "-OffEmail" in sys.argv or "--off-email" in sys.argv:
        EMAIL_DISABLED = True
        sys.argv = [arg for arg in sys.argv if arg not in ("-OffEmail", "--off-email")]

    # Флаг: отправлять письмо только на один адрес
    if "--debug-email" in sys.argv or "--debag-email" in sys.argv:
        EMAIL_DEBUG_RECIPIENT = _load_debug_email_from_config() or "eyenot2@yandex.ru"
        sys.argv = [arg for arg in sys.argv if arg not in ("--debug-email", "--debag-email")]


    # Флаг: статистика по выбранным спутникам за период
    if "--stat-commers" in sys.argv:
        EMAIL_DISABLED = True
        idx = sys.argv.index("--stat-commers")
        args = sys.argv[idx + 1 :]
        if not args:
            print(f"{Fore.RED}Ошибка: укажите станцию. Пример: --stat-commers R2.0S_Moscow 20250101 20250110")
            sys.exit(1)

        station_name = args[0]
        start_str = args[1] if len(args) >= 2 else datetime.now(timezone.utc).strftime("%Y%m%d")
        end_str = args[2] if len(args) >= 3 else start_str

        try:
            start_date = datetime.strptime(start_str, "%Y%m%d")
            end_date = datetime.strptime(end_str, "%Y%m%d")
        except ValueError:
            print(f"{Fore.RED}Ошибка: даты должны быть в формате YYYYMMDD")
            sys.exit(1)

        if end_date < start_date:
            print(f"{Fore.RED}Ошибка: конечная дата не может быть раньше начальной")
            sys.exit(1)

        satellites = _load_commercial_satellites()
        analyze_satellite_logs_for_station(
            station_name=station_name,
            start_date=start_date,
            end_date=end_date,
            satellites=satellites,
        )
        sys.exit(0)

    # Флаг: статистика по всем пролетам за период
    if "--stat-all" in sys.argv:
        EMAIL_DISABLED = True
        idx = sys.argv.index("--stat-all")
        args = sys.argv[idx + 1 :]
        if not args:
            print(f"{Fore.RED}Ошибка: укажите станцию. Пример: --stat-all R2.0S_Moscow 20250101 20250110")
            sys.exit(1)

        station_name = args[0]
        start_str = args[1] if len(args) >= 2 else datetime.now(timezone.utc).strftime("%Y%m%d")
        end_str = args[2] if len(args) >= 3 else start_str

        try:
            start_date = datetime.strptime(start_str, "%Y%m%d")
            end_date = datetime.strptime(end_str, "%Y%m%d")
        except ValueError:
            print(f"{Fore.RED}Ошибка: даты должны быть в формате YYYYMMDD")
            sys.exit(1)

        if end_date < start_date:
            print(f"{Fore.RED}Ошибка: конечная дата не может быть раньше начальной")
            sys.exit(1)

        analyze_satellite_logs_for_station(
            station_name=station_name,
            start_date=start_date,
            end_date=end_date,
            satellites=None,
        )
        sys.exit(0)

    # Проверяем, запущен ли скрипт в режиме планировщика
    if len(sys.argv) >= 2 and sys.argv[1] == "--scheduler":
        scheduler_loop()
        sys.exit(0)
    
    if len(sys.argv) < 2:
        # Если дата не указана, используем текущую дату в UTC
        start_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        print(f"{Fore.CYAN}Дата не указана, используется текущая дата (UTC): {start_date_str}")
    else:
        start_date_str = sys.argv[1]
    
    # Проверяем валидность первой даты
    try:
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
    except ValueError:
        print(f"{Fore.RED + Style.BRIGHT}Ошибка: неверный формат даты '{start_date_str}'. Ожидается формат YYYYMMDD")
        sys.exit(1)
    
    # Проверяем, указан ли диапазон дат
    if len(sys.argv) >= 3:
        end_date_str = sys.argv[2]
        try:
            end_date = datetime.strptime(end_date_str, "%Y%m%d")
        except ValueError:
            print(f"{Fore.RED + Style.BRIGHT}Ошибка: неверный формат даты '{end_date_str}'. Ожидается формат YYYYMMDD")
            sys.exit(1)
        
        # Проверяем, что конечная дата не раньше начальной
        if end_date < start_date:
            print(f"{Fore.RED + Style.BRIGHT}Ошибка: конечная дата не может быть раньше начальной")
            sys.exit(1)
        
        # Обрабатываем диапазон дат
        print(f"{Fore.CYAN + Style.BRIGHT}\nОБРАБОТКА ДИАПАЗОНА ДАТ")
        print(f"{Fore.CYAN}С {start_date_str} по {end_date_str}")
        
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        day_num = 0
        
        while current_date <= end_date:
            day_num += 1
            date_str = current_date.strftime("%Y%m%d")
            date_display = current_date.strftime("%d.%m.%Y")
            
            print(f"\n{Fore.CYAN + Style.BRIGHT}{'='*80}")
            print(f"{Fore.CYAN + Style.BRIGHT}ДЕНЬ {day_num} из {total_days}: {date_display} ({date_str})")
            print(f"{Fore.CYAN + Style.BRIGHT}{'='*80}\n")
            
            try:
    # Сначала скачиваем логи
                download_logs_for_date(date_str)
    
    # Затем анализируем скачанные логи
                analyze_downloaded_logs(date_str)
            except (FileNotFoundError, PermissionError) as e:
                logger.error(f"Ошибка доступа при обработке даты {date_str}: {e}")
                print_error(f"Ошибка доступа при обработке даты {date_str}: {e}", is_critical=False)
                print(f"{Fore.YELLOW}Продолжаем обработку следующей даты...")
            except (ValueError, KeyError) as e:
                logger.error(f"Ошибка данных при обработке даты {date_str}: {e}")
                print_error(f"Ошибка данных при обработке даты {date_str}: {e}", is_critical=False)
                print(f"{Fore.YELLOW}Продолжаем обработку следующей даты...")
            except Exception as e:
                logger.error(f"Неожиданная ошибка при обработке даты {date_str}: {e}", exc_info=True)
                print_error(f"Ошибка при обработке даты {date_str}: {e}", is_critical=False)
                print(f"{Fore.YELLOW}Продолжаем обработку следующей даты...")
            
            # Переходим к следующему дню
            current_date += timedelta(days=1)
        
        print(f"\n{Fore.GREEN + Style.BRIGHT}\nОБРАБОТКА ДИАПАЗОНА ДАТ ЗАВЕРШЕНА")
        print(f"{Fore.GREEN}Обработано дней: {total_days}")
    else:
        # Обрабатываем одну дату
        date_str = start_date_str
        
        # Сначала скачиваем логи
        download_logs_for_date(date_str)
        
        # Затем анализируем скачанные логи
        analyze_downloaded_logs(date_str)
