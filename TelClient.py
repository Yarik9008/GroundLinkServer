import asyncio
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
try:
    from telethon import TelegramClient  # type: ignore
except Exception:  # telethon is optional
    TelegramClient = None  # type: ignore[assignment]

# Алиасы станций по умолчанию (если в config нет telegram.station_aliases)
DEFAULT_STATION_ALIASES: Dict[str, str] = {
    "MUR": "R3.2S_Murmansk",
    "ANA": "R4.6S_Anadyr",
}

# Алиасы спутников по умолчанию (если в config нет telegram.satellite_aliases)
DEFAULT_SATELLITE_ALIASES: Dict[str, str] = {
    "B01": "JL1KF02B01",
    "B02": "JL1KF02B02",
    "B03": "JL1KF02B03",
    "B04": "JL1KF02B04",
    "B05": "JL1KF02B05",
    "B06": "JL1KF02B06",
    "B07": "JL1KF02B07",
    "TY-39": "TY-39",
    "TY-40": "TY-40",
    "TY-41": "TY-41",
    "TY-42": "TY-42",
}

COMM_STATION_ALIASES = DEFAULT_STATION_ALIASES  # обратная совместимость

# Регулярка для строки пролёта: станция спутник дата время_начала - время_окончания
COMM_PASS_LINE_RE = re.compile(
    r"^\s*(?P<station>\S+)\s+(?P<satellite>\S+)(?:\s+UTC)?\s+"
    r"(?P<date>\d{4}[./-]\d{2}[./-]\d{2})\s+"
    r"(?P<start>\d{2}:\d{2}:\d{2})\s*-\s*"
    r"(?P<end>\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)

# Тип: (station, satellite, session_start, session_end, pass_type, comment)
# pass_type: "коммерческий" | "тестовый коммерческий"
ParsedPass = Tuple[str, str, str, str, str, str]


class TelClient:
    """Клиент для чтения и парсинга коммерческих пролётов из Telegram-канала.

    Настройки: config["telegram"] или TG_API_ID, TG_API_HASH, TG_CHANNEL, TG_SESSION.
    Формат строки пролёта: станция спутник YYYY-MM-DD HH:MM:SS-HH:MM:SS.

    Методы:
        __init__: Инициализация с логгером и конфигом.
        _get_settings: Чтение настроек Telegram.
        _get_station_aliases, _get_satellite_aliases: Алиасы из config или defaults.
        _log: Логирование через self.logger.
        split_by_double_newline: Разбивка текста по \\n\\n.
        parse_passes: Парсинг строк пролётов в списке.
        parse_message: Парсинг сообщения (блоки по \\n\\n).
        _start_telegram_client: Запуск TelegramClient (async).
        iter_comm_messages: Итератор по сообщениям канала (async).
        sync_comm_passes_once: Один проход синхронизации (async).
        run_comm_passes_sync: Синхронная обёртка для main().

    Атрибуты:
        logger: Логгер.
        config: Конфигурация приложения.
    """

    # Инициализация клиента с логгером и конфигурацией.
    def __init__(
        self,
        logger: Optional[Any] = None,
        config: Optional[Dict[str, Any]] = None,
        ) -> None:
        """Инициализирует клиент.

        Args:
            logger: Логгер (опционально).
            config: Конфигурация (config["telegram"] для настроек).
        """
        self.logger = logger
        self.config = config or {}

    # Чтение настроек Telegram из config и переменных окружения.
    def _get_settings(self) -> Dict[str, Any]:
        """Читает настройки Telegram (api_id, api_hash, channel, session).

        Returns:
            dict с ключами api_id, api_hash, channel, session.
        """
        tg = self.config.get("telegram") or {}
        base_dir = os.path.dirname(os.path.abspath(__file__))
        api_id_raw = tg.get("api_id") or os.getenv("TG_API_ID", "25004944")
        api_hash = tg.get("api_hash") or os.getenv("TG_API_HASH", "3d29770555fbca4b0ea880003ed892bc")
        channel = tg.get("channel") or os.getenv("TG_CHANNEL", "")
        session = tg.get("session") or os.getenv("TG_SESSION", str(Path(base_dir) / "telegram"))
        try:
            api_id = int(api_id_raw)
        except (TypeError, ValueError):
            api_id = 0
        return {
            "api_id": api_id,
            "api_hash": str(api_hash) if api_hash else "",
            "channel": str(channel).strip() if channel else "",
            "session": str(session),
        }

    # Алиасы станций: config или DEFAULT_STATION_ALIASES.
    def _get_station_aliases(self) -> Dict[str, str]:
        """Алиасы станций из config.telegram.station_aliases или config.comm_station_aliases, иначе по умолчанию."""
        tg = self.config.get("telegram") or {}
        aliases = tg.get("station_aliases") or self.config.get("comm_station_aliases")
        return dict(aliases) if aliases else dict(DEFAULT_STATION_ALIASES)

    # Алиасы спутников: config или DEFAULT_SATELLITE_ALIASES.
    def _get_satellite_aliases(self) -> Dict[str, str]:
        """Алиасы спутников из config.telegram.satellite_aliases, иначе по умолчанию."""
        tg = self.config.get("telegram") or {}
        aliases = tg.get("satellite_aliases")
        return dict(aliases) if aliases else dict(DEFAULT_SATELLITE_ALIASES)

    # Логирование через self.logger (поддержка % и .format).
    def _log(self, level: str, msg: str, *args: Any, **kwargs: Any) -> None:
        if self.logger is None:
            return
        # Logger в проекте принимает один аргумент (сообщение), форматируем здесь
        if args or kwargs:
            try:
                msg = msg % args if args else msg.format(**kwargs)
            except (TypeError, KeyError):
                msg = msg + " " + " ".join(str(a) for a in args)
        getattr(self.logger, level, self.logger.info)(msg)

    # Разбивка текста по двойному переносу строки.
    @staticmethod
    def split_by_double_newline(text: str) -> List[str]:
        """Делит текст по двойному переносу строки, возвращает непустые куски."""
        chunks = [part.strip() for part in text.split("\n\n")]
        return [part for part in chunks if part]

    # Парсинг строк пролётов в списке (station, satellite, session_start, session_end).
    def parse_passes(self, text: str, *, default_pass_type: str = "коммерческий") -> List[ParsedPass]:
        """
        Парсит текст сообщения и возвращает список пролётов.
        Каждый элемент — (station, satellite, session_start, session_end, pass_type, comment).
        """
        passes: List[ParsedPass] = []
        test_re = re.compile(r"\b(тест|test)\w*\b", re.IGNORECASE)
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            match = COMM_PASS_LINE_RE.match(line)
            if not match:
                continue
            tail = line[match.end():].strip()
            is_test = bool(test_re.search(tail)) or bool(test_re.search(line))
            pass_type = "тестовый коммерческий" if is_test else default_pass_type
            comment = tail
            station = match.group("station")
            station = self._get_station_aliases().get(station, station)
            satellite = match.group("satellite")
            satellite = self._get_satellite_aliases().get(satellite, satellite)
            date = match.group("date").replace(".", "-").replace("/", "-")
            start_time = match.group("start")
            end_time = match.group("end")
            session_start = f"{date} {start_time}"
            session_end = f"{date} {end_time}"
            passes.append((station, satellite, session_start, session_end, pass_type, comment))
        return passes

    # Парсинг сообщения: разбивка по \\n\\n и парсинг каждого блока.
    def parse_message(self, text: str) -> List[ParsedPass]:
        """
        Делит сообщение по двойному переносу, парсит каждый блок и возвращает
        объединённый список пролётов.
        """
        parts = self.split_by_double_newline(text)
        result: List[ParsedPass] = []
        for part in parts:
            # Если в блоке есть "тест"/"test", считаем пролёты тестовыми по умолчанию
            is_test_block = bool(re.search(r"\b(тест|test)\w*\b", part, flags=re.IGNORECASE))
            default_type = "тестовый коммерческий" if is_test_block else "коммерческий"
            result.extend(self.parse_passes(part, default_pass_type=default_type))
        return result

    # Запуск TelegramClient (fallback на сессию с pid при database is locked).
    async def _start_telegram_client(self) -> Any:
        """Запускает TelegramClient. При database is locked использует сессию с суффиксом pid."""
        if TelegramClient is None:
            raise RuntimeError("telethon не установлен. Установите: pip install telethon")
        settings = self._get_settings()
        if not settings["api_id"] or not settings["api_hash"]:
            raise ValueError("TG_API_ID и TG_API_HASH должны быть заданы")
        client = None
        try:
            client = TelegramClient(
                settings["session"],
                settings["api_id"],
                settings["api_hash"],
            )
            await client.start()
            return client
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc).lower():
                raise
            if client is not None:
                await client.disconnect()
            fallback_session = f"{settings['session']}_pid{os.getpid()}"
            client = TelegramClient(fallback_session, settings["api_id"], settings["api_hash"])
            await client.start()
            return client

    # Асинхронный итератор по сообщениям канала.
    async def iter_comm_messages(
        self,
        client: Optional[Any] = None,
        ) -> AsyncIterator[Tuple[int, str, List[str], List[ParsedPass]]]:
        """
        Асинхронный генератор: для каждого сообщения канала выдаёт
        (индекс, текст, части по \\n\\n, список распарсенных пролётов).
        client можно не передавать — тогда создаётся и закрывается внутри.
        """
        if TelegramClient is None:
            raise RuntimeError("telethon не установлен. Установите: pip install telethon")
        settings = self._get_settings()
        if not settings["api_id"] or not settings["api_hash"] or not settings["channel"]:
            raise ValueError("Задайте TG_API_ID, TG_API_HASH, TG_CHANNEL (или config.telegram)")
        own_client = client is None
        if own_client:
            client = await self._start_telegram_client()
        try:
            index = 0
            async for msg in client.iter_messages(settings["channel"], reverse=True):
                text = msg.message or ""
                parts = self.split_by_double_newline(text)
                passes = self.parse_message(text)
                yield index, text, parts, passes
                index += 1
        finally:
            if own_client and client is not None:
                await client.disconnect()

    # Один проход: чтение всех сообщений, парсинг, возврат (msgs, passes_count, passes_list).
    async def sync_comm_passes_once(self) -> Tuple[int, int, List[ParsedPass]]:
        """
        Один проход: читает все сообщения канала, парсит и возвращает
        (число сообщений, число распарсенных пролётов, список пролётов). БД не используется.
        """
        if TelegramClient is None:
            raise RuntimeError("telethon не установлен. Установите: pip install telethon")
        settings = self._get_settings()
        if not settings["api_id"] or not settings["api_hash"] or not settings["channel"]:
            raise ValueError("Задайте TG_API_ID, TG_API_HASH, TG_CHANNEL (или config.telegram)")
        self._log("info", "Синхронизация Telegram канала: %s", settings["channel"])
        total_msgs = 0
        all_passes: List[ParsedPass] = []
        async for _idx, _text, _parts, passes in self.iter_comm_messages():
            total_msgs += 1
            all_passes.extend(passes)
        self._log("info", "Синхронизация завершена: сообщений=%s, пролётов=%s", total_msgs, len(all_passes))
        return total_msgs, len(all_passes), all_passes

    # Синхронная обёртка: asyncio.run(sync_comm_passes_once).
    def run_comm_passes_sync(self) -> Optional[Tuple[int, int, List[ParsedPass]]]:
        """
        Синхронная обёртка: запускает sync_comm_passes_once() в asyncio.
        Возвращает (total_msgs, total_passes, список пролётов) или None при ошибке/отключении.
        """
        if TelegramClient is None:
            self._log("warning", "telethon не установлен, синхронизация Telegram отключена")
            return None
        settings = self._get_settings()
        if not settings["api_id"] or not settings["api_hash"] or not settings["channel"]:
            self._log("warning", "TG_API_ID/TG_API_HASH/TG_CHANNEL не заданы, синхронизация Telegram отключена")
            return None
        try:
            return asyncio.run(self.sync_comm_passes_once())
        except EOFError:
            self._log(
                "warning",
                "Требуется интерактивный ввод (номер телефона, код). Запустите скрипт из терминала: python TelClient.py",
            )
            return None
        except Exception as exc:
            self._log("warning", "Ошибка синхронизации Telegram: %s", exc)
            if self.logger and hasattr(self.logger, "exception"):
                self.logger.exception("Ошибка синхронизации Telegram")
            return None


if __name__ == "__main__":
    import json
    import logging
    # Чтобы видеть причину ошибки при запуске из консоли
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    _logger = logging.getLogger("TelClient")

    # Загружаем config.json из каталога скрипта (рядом с TelClient.py), затем из текущего каталога
    script_dir = Path(__file__).resolve().parent
    config_paths = [
        script_dir / "config.json",
        Path.cwd() / "config.json",
    ]
    config = {}
    for config_path in config_paths:
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            break
        except (FileNotFoundError, json.JSONDecodeError):
            continue
    client = TelClient(logger=_logger, config=config)
    result = client.run_comm_passes_sync()
    if result is not None:
        total_msgs, total_passes, passes_list = result
        print(f"Синхронизация: сообщений={total_msgs}, пролётов={total_passes}")
        if passes_list:
            print("\nСписок пролётов:")
            for station, satellite, session_start, session_end, pass_type, comment in passes_list:
                suffix = f" [{pass_type}]" if pass_type else ""
                comment_s = f" ({comment})" if comment else ""
                print(f"  {station}  {satellite}  {session_start} — {session_end}{suffix}{comment_s}")
    else:
        tg = config.get("telegram") or {}
        has_telegram = bool(tg.get("api_id") and tg.get("api_hash") and tg.get("channel"))
        if has_telegram:
            print("Синхронизация не выполнена: установите telethon (pip install telethon), проверьте доступ к каналу.")
            print("При первом запуске нужна интерактивная авторизация: введите номер телефона и код из Telegram (запустите скрипт из терминала).")
        else:
            print("Синхронизация не выполнена: задайте TG_API_ID, TG_API_HASH, TG_CHANNEL в config.telegram или в переменных окружения (и установите telethon).")