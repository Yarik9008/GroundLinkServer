import asyncio
import json
import os
import re
import sqlite3
from pathlib import Path

from telethon import TelegramClient, events

BASE_DIR = Path(__file__).resolve().parent.parent


def load_telegram_comm_config() -> dict:
    config_path = BASE_DIR / "config.json"
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data.get("telegram_comm", {}) if isinstance(data, dict) else {}


_comm_cfg = load_telegram_comm_config()
API_ID = int(os.getenv("TG_API_ID", _comm_cfg.get("api_id", 25004944)))
API_HASH = os.getenv("TG_API_HASH", _comm_cfg.get("api_hash", "3d29770555fbca4b0ea880003ed892bc"))
CHANNEL = os.getenv("TG_CHANNEL", _comm_cfg.get("channel", "https://t.me/+Lb1SuoOUlodhYTli"))
SESSION = os.getenv("TG_SESSION", _comm_cfg.get("session", str(BASE_DIR / "telegram")))
DB_PATH = os.getenv("COMM_DB_PATH", _comm_cfg.get("db_path", str(BASE_DIR / "comm_passes.db")))


def split_by_double_newline(text: str) -> list[str]:
    chunks = [part.strip() for part in text.split("\n\n")]
    return [part for part in chunks if part]


# Перевод коротких кодов станций в полные имена.
STATION_ALIASES = {
    "MUR": "R3.2S_Murmansk",
    "ANA": "R4.6S_Anadyr",
}

# Регулярка для строк пролетов (поддерживает UTC и разные разделители дат).
PASS_LINE_RE = re.compile(
    r"^\s*(?P<station>\S+)\s+(?P<satellite>\S+)(?:\s+UTC)?\s+"
    r"(?P<date>\d{4}[./-]\d{2}[./-]\d{2})\s+"
    r"(?P<start>\d{2}:\d{2}:\d{2})\s*-\s*"
    r"(?P<end>\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)


def init_db(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def quote_identifier(name: str) -> str:
    # Безопасное экранирование идентификаторов для SQLite.
    return '"' + name.replace('"', '""') + '"'


def ensure_station_table(conn: sqlite3.Connection, station: str) -> None:
    table_name = quote_identifier(station)
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


def add_pass(
    conn: sqlite3.Connection,
    station: str,
    satellite: str,
    session_start: str,
    session_end: str,
) -> None:
    ensure_station_table(conn, station)
    table_name = quote_identifier(station)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT 1
        FROM {table_name}
        WHERE satellite = ?
          AND session_start = ?
          AND session_end = ?
        LIMIT 1
        """,
        (satellite, session_start, session_end),
    )
    if cursor.fetchone() is not None:
        cursor.close()
        return
    cursor.execute(
        f"INSERT INTO {table_name} (satellite, session_start, session_end) VALUES (?, ?, ?)",
        (satellite, session_start, session_end),
    )
    conn.commit()
    cursor.close()


def list_passes(conn: sqlite3.Connection, station: str) -> list[tuple[int, str, str, str]]:
    table_name = quote_identifier(station)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT id, satellite, session_start, session_end
        FROM {table_name}
        ORDER BY datetime(session_start)
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def parse_passes(text: str) -> list[tuple[str, str, str, str]]:
    # Возвращает список пролетов, найденных в тексте.
    passes: list[tuple[str, str, str, str]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        match = PASS_LINE_RE.match(line)
        if not match:
            continue
        station = match.group("station")
        station = STATION_ALIASES.get(station, station)
        satellite = match.group("satellite")
        date = match.group("date").replace(".", "-").replace("/", "-")
        start_time = match.group("start")
        end_time = match.group("end")
        session_start = f"{date} {start_time}"
        session_end = f"{date} {end_time}"
        passes.append((station, satellite, session_start, session_end))
    return passes


def insert_passes_from_parts(conn: sqlite3.Connection, parts: list[str]) -> set[str]:
    # Парсит части сообщений и сохраняет найденные пролеты.
    stations: set[str] = set()
    for part in parts:
        for station, satellite, start, end in parse_passes(part):
            add_pass(conn, station, satellite, start, end)
            stations.add(station)
    return stations


async def start_telegram_client() -> TelegramClient:
    # Если сессия занята (locked), используем альтернативное имя.
    client: TelegramClient | None = None
    try:
        client = TelegramClient(SESSION, API_ID, API_HASH)
        await client.start()
        return client
    except sqlite3.OperationalError as exc:
        if "database is locked" not in str(exc).lower():
            raise
        if client is not None:
            await client.disconnect()
        fallback_session = f"{SESSION}_pid{os.getpid()}"
        client = TelegramClient(fallback_session, API_ID, API_HASH)
        await client.start()
        return client


async def main() -> None:
    if not API_ID or not API_HASH or not CHANNEL:
        raise SystemExit("Set TG_API_ID, TG_API_HASH, TG_CHANNEL")

    # Подключение к Telegram и подготовка БД.
    client = await start_telegram_client()
    db_conn = init_db(DB_PATH)

    messages: list[str] = []
    print(f"History for: {CHANNEL}")
    async for msg in client.iter_messages(CHANNEL, reverse=True):
        text = msg.message or ""
        parts = split_by_double_newline(text)
        messages.extend(parts)
        print(f"HISTORY: {parts}")
        insert_passes_from_parts(db_conn, parts)

    print("HISTORY_LIST:", messages)

    @client.on(events.NewMessage(chats=CHANNEL))
    async def on_new(event: events.NewMessage.Event) -> None:
        text = event.message.message or ""
        parts = split_by_double_newline(text)
        messages.extend(parts)
        print(f"NEW: {parts}")
        insert_passes_from_parts(db_conn, parts)

    @client.on(events.MessageEdited(chats=CHANNEL))
    async def on_edit(event: events.MessageEdited.Event) -> None:
        text = event.message.message or ""
        parts = split_by_double_newline(text)
        messages.extend(parts)
        print(f"EDITED: {parts}")
        insert_passes_from_parts(db_conn, parts)

    print(f"Watching new + edits in: {CHANNEL}")
    print("Все пролеты в базе по станциям:")
    for station in sorted(STATION_ALIASES.values()):
        try:
            rows = list_passes(db_conn, station)
        except sqlite3.OperationalError:
            continue
        if not rows:
            continue
        print(f"Станция {station}:")
        for row in rows:
            print(row)

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
