import sqlite3
from datetime import date, datetime, time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from SatPass import SatPas
from Logger import Logger


class DbManager:
    """SQLite менеджер для станций, пролетов и дневной статистики.

    Назначение:
        - Инициализирует и поддерживает схему БД.
        - Сохраняет данные пролетов и коммерческих пролетов.
        - Возвращает агрегированную статистику по дням/станциям.

    Методы:
        - __init__: инициализация менеджера и схемы БД.
        - _ensure_parent_dir: создает каталог для файла БД.
        - _connect: открывает соединение SQLite.
        - _init_schema: создает таблицы/индексы при необходимости.
        - _normalize_date: нормализация даты к ISO-строке.
        - _normalize_time: нормализация времени к строке.
        - _normalize_datetime: нормализация даты/времени к строке.
        - _combine_date_time: склеивание даты и времени в строку.
        - add_pass: добавляет пролет и обновляет дневную статистику.
        - _bump_stats: обновляет дневную статистику станции.
        - add_commercial_pass: добавляет коммерческий пролет.
        - replace_commercial_passes: заменяет все коммерческие пролёты списком из Telegram.
        - get_commercial_passes_planned_count: число коммерческих пролётов за день (опционально до момента UTC).
        - get_commercial_passes_stats_by_station: статистика по станциям (planned/successful/not_received) для письма.
        - get_commercial_passes_received_count: число коммерческих пролётов за день с успешным приёмом.
        - get_commercial_passes_not_received_list: список коммерческих пролётов без приёма (для письма).
        - list_passes: возвращает список пролетов с фильтром по станции.
        - get_daily_success_stats: статистика успешности за день.
        - get_daily_station_stats: статистика по станциям за день.
        - get_max_snr_sum_passes: пролет с max snr_sum по станции за день.
        - get_failed_graphs_by_station: графики пустых пролетов по станциям за день.
    """

    def __init__(
        self,
        logger: Logger,
        db_path: str | Path = "groundlink.db",
        ) -> None:
        """Инициализирует менеджер и задает параметры подключения.

        Args:
            logger: Объект логгера.
            db_path: Путь к файлу SQLite.
        """
        self.db_path = str(db_path)  # путь к базе данных
        self.logger = logger  # объект логгера
        self._ensure_parent_dir()
        self._conn: Optional[sqlite3.Connection] = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA foreign_keys = ON;")
        self._init_schema()

    # Создает каталог для файла базы данных, если он не существует.
    def _ensure_parent_dir(self) -> None:
        """Создает каталог для файла базы данных."""
        parent = Path(self.db_path).parent
        parent.mkdir(parents=True, exist_ok=True)

    # Открывает соединение с SQLite и включает foreign_keys.
    def _connect(self) -> sqlite3.Connection:
        """Возвращает текущее соединение SQLite."""
        if self._conn is None:
            raise RuntimeError("SQLite connection is closed")
        return self._conn

    # Закрывает соединение с SQLite.
    def close(self) -> None:
        """Закрывает соединение с SQLite."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # Создает таблицы и индексы при отсутствии.
    def _init_schema(self) -> None:
        """Создает таблицы и индексы при отсутствии."""
        conn = self._connect()
        try:
            conn.executescript(
                """
                DROP TABLE IF EXISTS commercial_passes;
                DROP VIEW IF EXISTS commercial_passes;

                CREATE TABLE IF NOT EXISTS all_passes (
                    id INTEGER PRIMARY KEY,
                    pass_id TEXT,
                    station_name TEXT NOT NULL,
                    satellite_name TEXT NOT NULL,
                    location TEXT,
                    pass_date DATE NOT NULL,
                    pass_start_time TEXT NOT NULL,
                    pass_end_time TEXT,
                    rx_start_time TEXT,
                    rx_end_time TEXT,
                    snr_awg REAL,
                    snr_max REAL,
                    snr_sum REAL,
                    log_url TEXT,
                    log_path TEXT,
                    graph_url TEXT,
                    graph_path TEXT,
                    success INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS commercial_passes (
                    id INTEGER PRIMARY KEY,
                    station_name TEXT NOT NULL,
                    satellite_name TEXT NOT NULL,
                    rx_start_time TEXT NOT NULL,
                    rx_end_time TEXT,
                    pass_type TEXT NOT NULL,
                    comment TEXT
                );

                CREATE TABLE IF NOT EXISTS station_stats (
                    id INTEGER PRIMARY KEY,
                    station_name TEXT NOT NULL,
                    stat_day DATE NOT NULL,
                    total_passes INTEGER NOT NULL DEFAULT 0,
                    success_passes INTEGER NOT NULL DEFAULT 0,
                    failed_passes INTEGER NOT NULL DEFAULT 0,
                    failed_percent REAL NOT NULL DEFAULT 0,
                    comment TEXT,
                    UNIQUE (station_name, stat_day)
                );

                CREATE INDEX IF NOT EXISTS idx_all_passes_station_date
                    ON all_passes(station_name, pass_date);
                CREATE INDEX IF NOT EXISTS idx_all_passes_satellite
                    ON all_passes(satellite_name);
                CREATE INDEX IF NOT EXISTS idx_stats_station_day
                    ON station_stats(station_name, stat_day);
                """
            )
            # проверяем, что колонки существуют в таблице all_passes
            columns = [row[1] for row in conn.execute("PRAGMA table_info(all_passes)")]
            if "pass_id" not in columns:
                conn.execute("ALTER TABLE all_passes ADD COLUMN pass_id TEXT")
            if "location" not in columns:
                conn.execute("ALTER TABLE all_passes ADD COLUMN location TEXT")
            if "snr_awg" not in columns:
                conn.execute("ALTER TABLE all_passes ADD COLUMN snr_awg REAL")
            if "snr_max" not in columns:
                conn.execute("ALTER TABLE all_passes ADD COLUMN snr_max REAL")
            if "snr_sum" not in columns:
                conn.execute("ALTER TABLE all_passes ADD COLUMN snr_sum REAL")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        self.logger.info("DbManager initialized")

    # Приводит дату к ISO-строке.
    def _normalize_date(self, value: date | datetime | str) -> str:
        """Приводит дату к ISO-строке."""
        # если значение является datetime объектом, то возвращает дату в формате ISO
        if isinstance(value, datetime):
            return value.date().isoformat()
        # если значение является date объектом, то возвращает дату в формате ISO    
        if isinstance(value, date):
            return value.isoformat()
        # если значение является строкой, то возвращает строку
        return str(value)

    # Преобразует значение из БД (строка/date/datetime) в date.
    def _parse_date(self, value: date | datetime | str | None) -> date | None:
        """Преобразует дату из БД в объект date (для SatPas.pass_date)."""
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        s = str(value).strip()
        if not s:
            return None
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            try:
                return datetime.strptime(s[:8], "%Y%m%d").date()
            except ValueError:
                return None

    # Округляет вещественные числа до 2 знаков.
    def _round2(self, value: Optional[float]) -> Optional[float]:
        """Округляет значение до 2 знаков, если оно задано."""
        if value is None:
            return None
        try:
            return round(float(value), 2)
        except (TypeError, ValueError):
            return None

    # Приводит время к строке.
    def _normalize_time(self, value: time | datetime | str | None) -> Optional[str]:
        """Приводит время к строке."""
        # если значение не задано, то возвращаем None
        if value is None:
            return None
        # если значение является datetime объектом, то возвращаем время в формате ISO
        if isinstance(value, datetime):
            return value.time().isoformat(timespec="seconds")
        # если значение является time объектом, то возвращаем время в формате ISO
        if isinstance(value, time):
            return value.isoformat(timespec="seconds")
        # если значение является строкой, то возвращаем строку
        return str(value)

    # Приводит дату/время к формату YYYY-MM-DD HH:MM:SS.
    def _normalize_datetime(self, value: datetime | date | time | str | None) -> Optional[str]:
        """Приводит дату/время к формату YYYY-MM-DD HH:MM:SS."""
        # если значение не задано, то возвращаем None
        if value is None:
            return None
        # если значение является datetime объектом, то возвращаем дату/время в формате YYYY-MM-DD HH:MM:SS
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        # если значение является date объектом, то возвращает дату/время в формате YYYY-MM-DD HH:MM:SS
        if isinstance(value, date):
            return datetime.combine(value, time.min).strftime("%Y-%m-%d %H:%M:%S")
        # если значение является time объектом, то возвращает дату/время в формате YYYY-MM-DD HH:MM:SS
        if isinstance(value, time):
            return datetime.combine(date.today(), value).strftime("%Y-%m-%d %H:%M:%S")
        # если значение является строкой, то возвращает строку
        return str(value)

    # Склеивает дату и время в формат YYYY-MM-DD HH:MM:SS.
    def _combine_date_time(
        self,
        pass_date: date | datetime | str,
        value: time | datetime | str | None,
        ) -> Optional[str]:
        """Склеивает дату и время в формат YYYY-MM-DD HH:MM:SS."""
        # если значение не задано, то возвращаем None
        if value is None:
            return None
        # если значение является datetime объектом, то возвращает дату/время в формате YYYY-MM-DD HH:MM:SS
            return None
        # если значение является datetime объектом, то возвращает дату/время в формате YYYY-MM-DD HH:MM:SS
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        # если значение является time объектом, то возвращает дату/время в формате YYYY-MM-DD HH:MM:SS
        if isinstance(value, time):
            base_date = pass_date.date() if isinstance(pass_date, datetime) else pass_date
            if isinstance(base_date, str):
                base_date = date.fromisoformat(base_date)
            return datetime.combine(base_date, value).strftime("%Y-%m-%d %H:%M:%S")
        # если значение является строкой, то возвращает строку
        return str(value)

    # Добавляет пролет и обновляет дневную статистику.
    def add_pass(
        self,
        sat_pass: SatPas,
        is_commercial: bool = False,
        success: Optional[bool] = None,
        ) -> int:

        """Добавляет пролет и обновляет дневную статистику.

        Args:
            sat_pass: Объект SatPas с данными пролета.
            is_commercial: Флаг коммерческого пролета (в текущей таблице не используется).
            success: Успешный пролет или нет (если None, берется из sat_pass.success).

        Returns:
            int: ID добавленного пролета.
        """
        # если название станции или название спутника не задано, то выбрасываем ошибку
        if not sat_pass.station_name or not sat_pass.satellite_name:
            raise ValueError("SatPas.station_name and SatPas.satellite_name are required")
        # если дата пролета или время начала пролета не задано, то выбрасываем ошибку
        if sat_pass.pass_date is None or sat_pass.pass_start_time is None:
            raise ValueError("SatPas.pass_date and SatPas.pass_start_time are required")
        # если успех пролета не задано, то берется из sat_pass.success
        success_flag = sat_pass.success if success is None else success
        # соединяемся с базой данных
        conn = self._connect()
        try:
            # если pass_id задано, то проверяем, что пролет уже существует
            if sat_pass.pass_id:
                # проверяем, что пролет уже существует
                existing = conn.execute(
                    "SELECT id FROM all_passes WHERE pass_id = ? LIMIT 1",
                    (sat_pass.pass_id,),
                ).fetchone() # возвращается строка с id пролета
                if existing:
                    self.logger.info(f"Pass with pass_id {sat_pass.pass_id} already exists")
                    return int(existing[0]) # возвращается id пролета
            cur = conn.execute(
                """
                INSERT INTO all_passes (
                    pass_id, station_name, satellite_name, location, pass_date,
                    pass_start_time, pass_end_time,
                    rx_start_time, rx_end_time,
                    snr_awg, snr_max, snr_sum,
                    log_url, log_path, graph_url, graph_path,
                    success
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sat_pass.pass_id,
                    sat_pass.station_name,
                    sat_pass.satellite_name,
                    str(sat_pass.location) if sat_pass.location is not None else None,
                    self._normalize_date(sat_pass.pass_date),
                    self._combine_date_time(sat_pass.pass_date, sat_pass.pass_start_time),
                    self._combine_date_time(sat_pass.pass_date, sat_pass.pass_end_time),
                    self._combine_date_time(sat_pass.pass_date, sat_pass.rx_start_time),
                    self._combine_date_time(sat_pass.pass_date, sat_pass.rx_end_time),
                    self._round2(sat_pass.snr_awg),
                    self._round2(sat_pass.snr_max),
                    self._round2(sat_pass.snr_sum),
                    sat_pass.log_url,
                    sat_pass.log_path,
                    sat_pass.graph_url,
                    sat_pass.graph_path,
                    1 if success_flag else 0,
                ),
            )
            self._bump_stats(
                conn,
                sat_pass.station_name,
                self._normalize_date(sat_pass.pass_date),
                is_success=success_flag,
            )
            conn.commit()
            return int(cur.lastrowid)
        except Exception:
            conn.rollback()
            raise

    # Добавляет несколько пролетов за одну транзакцию.
    def add_passes_batch(self, sat_passes: Iterable[SatPas]) -> int:
        """Добавляет несколько пролетов за одну транзакцию."""
        conn = self._connect()
        rows_to_insert = []
        stats_inc = {}
        pass_ids = [p.pass_id for p in sat_passes if p.pass_id]
        existing_ids = set()

        try:
            if pass_ids:
                chunk_size = 900
                for i in range(0, len(pass_ids), chunk_size):
                    chunk = pass_ids[i:i + chunk_size]
                    placeholders = ",".join(["?"] * len(chunk))
                    rows = conn.execute(
                        f"SELECT pass_id FROM all_passes WHERE pass_id IN ({placeholders})",
                        chunk,
                    ).fetchall()
                    existing_ids.update(r[0] for r in rows)

            for sat_pass in sat_passes:
                if not sat_pass.station_name or not sat_pass.satellite_name:
                    continue
                if sat_pass.pass_date is None or sat_pass.pass_start_time is None:
                    continue
                if sat_pass.pass_id and sat_pass.pass_id in existing_ids:
                    continue

                success_flag = bool(sat_pass.success)
                rows_to_insert.append(
                    (
                        sat_pass.pass_id,
                        sat_pass.station_name,
                        sat_pass.satellite_name,
                        str(sat_pass.location) if sat_pass.location is not None else None,
                        self._normalize_date(sat_pass.pass_date),
                        self._combine_date_time(sat_pass.pass_date, sat_pass.pass_start_time),
                        self._combine_date_time(sat_pass.pass_date, sat_pass.pass_end_time),
                        self._combine_date_time(sat_pass.pass_date, sat_pass.rx_start_time),
                        self._combine_date_time(sat_pass.pass_date, sat_pass.rx_end_time),
                        self._round2(sat_pass.snr_awg),
                        self._round2(sat_pass.snr_max),
                        self._round2(sat_pass.snr_sum),
                        sat_pass.log_url,
                        sat_pass.log_path,
                        sat_pass.graph_url,
                        sat_pass.graph_path,
                        1 if success_flag else 0,
                    )
                )

                stat_day = self._normalize_date(sat_pass.pass_date)
                key = (sat_pass.station_name, stat_day)
                if key not in stats_inc:
                    stats_inc[key] = {"total": 0, "success": 0, "failed": 0}
                stats_inc[key]["total"] += 1
                if success_flag:
                    stats_inc[key]["success"] += 1
                else:
                    stats_inc[key]["failed"] += 1

            if rows_to_insert:
                conn.executemany(
                    """
                    INSERT INTO all_passes (
                        pass_id, station_name, satellite_name, location, pass_date,
                        pass_start_time, pass_end_time,
                        rx_start_time, rx_end_time,
                        snr_awg, snr_max, snr_sum,
                        log_url, log_path, graph_url, graph_path,
                        success
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows_to_insert,
                )

            for (station_name, day_value), inc in stats_inc.items():
                total_inc = inc["total"]
                success_inc = inc["success"]
                failed_inc = inc["failed"]
                conn.execute(
                    """
                    INSERT INTO station_stats (
                        station_name, stat_day, total_passes, success_passes, failed_passes, failed_percent, comment
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(station_name, stat_day) DO UPDATE SET
                        total_passes = total_passes + ?,
                        success_passes = success_passes + ?,
                        failed_passes = failed_passes + ?,
                        failed_percent = CASE
                            WHEN (total_passes + ?) > 0 THEN
                                ROUND(
                                    ((failed_passes + ?) * 100.0) / (total_passes + ?),
                                    2
                                )
                            ELSE 0
                        END
                    """,
                    (
                        station_name,
                        day_value,
                        total_inc,
                        success_inc,
                        failed_inc,
                        self._round2((failed_inc * 100.0) / total_inc) if total_inc else 0.0,
                        None,
                        total_inc,
                        success_inc,
                        failed_inc,
                        total_inc,
                        failed_inc,
                        total_inc,
                    ),
                )

            conn.commit()
            return len(rows_to_insert)
        except Exception:
            conn.rollback()
            raise

    # Автоматически обновляет дневную статистику по станции.
    def _bump_stats(
        self,
        conn: sqlite3.Connection,
        station_name: str,
        day_value: str,
        is_success: bool,
        ) -> None:
        """Автоматически обновляет дневную статистику по станции."""
        total_inc = 1
        success_inc = 1 if is_success else 0
        failed_inc = 0 if is_success else 1
        conn.execute(
            """
            INSERT INTO station_stats (
                station_name, stat_day, total_passes, success_passes, failed_passes, failed_percent, comment
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_name, stat_day) DO UPDATE SET
                total_passes = total_passes + ?,
                success_passes = success_passes + ?,
                failed_passes = failed_passes + ?,
                failed_percent = CASE
                    WHEN (total_passes + ?) > 0 THEN
                        ROUND(
                            ((failed_passes + ?) * 100.0) / (total_passes + ?),
                            2
                        )
                    ELSE 0
                END
            """,
            (
                station_name,
                day_value,
                total_inc,
                success_inc,
                failed_inc,
                self._round2((failed_inc * 100.0) / total_inc) if total_inc else 0.0,
                None,
                total_inc,
                success_inc,
                failed_inc,
                total_inc,
                failed_inc,
                total_inc,
            ),
        )

    # Добавляет заказанный коммерческий пролет.
    def add_commercial_pass(
        self,
        station_name: str,
        satellite_name: str,
        pass_type: str,
        rx_start_time: time | datetime | str,
        rx_end_time: Optional[time | datetime | str] = None,
        comment: Optional[str] = None,
        ) -> int:
        """Добавляет заказанный коммерческий пролет.

        Args:
            station_name: Название станции.
            satellite_name: Название спутника.
            pass_type: Тип пролета (тестовый/комерческий).
            rx_start_time: Время начала приема.
            rx_end_time: Время завершения приема.
            comment: Комментарий.

        Returns:
            int: ID добавленного коммерческого пролета.
        """
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                INSERT INTO commercial_passes (
                    station_name, satellite_name, rx_start_time, rx_end_time, pass_type, comment
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    station_name,
                    satellite_name,
                    self._normalize_time(rx_start_time),
                    self._normalize_time(rx_end_time),
                    pass_type,
                    comment,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
        except Exception:
            conn.rollback()
            raise

    # Заменяет все коммерческие пролёты списком из Telegram.
    def replace_commercial_passes(
        self,
        passes: List[Tuple[str, str, str, str]],
        pass_type: str = "коммерческий",
    ) -> int:
        """Заменяет все записи в commercial_passes на переданный список.

        Каждый элемент passes — (station_name, satellite_name, rx_start_time, rx_end_time).

        Returns:
            int: Количество записанных пролётов.
        """
        conn = self._connect()
        try:
            conn.execute("DELETE FROM commercial_passes")
            count = 0
            for station_name, satellite_name, rx_start, rx_end in passes:
                conn.execute(
                    """
                    INSERT INTO commercial_passes (
                        station_name, satellite_name, rx_start_time, rx_end_time, pass_type
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        station_name,
                        satellite_name,
                        self._normalize_time(rx_start),
                        self._normalize_time(rx_end),
                        pass_type,
                    ),
                )
                count += 1
            conn.commit()
            return count
        except Exception:
            conn.rollback()
            raise

    # Возвращает список пролетов (опционально по станции).
    def list_passes(self, station_name: Optional[str] = None) -> Iterable[sqlite3.Row]:
        """Возвращает список пролетов (опционально по станции).

        Args:
            station_name: Название станции для фильтрации.

        Returns:
            Iterable[sqlite3.Row]: Список строк из all_passes.
        """
        conn = self._connect()
        prev_factory = conn.row_factory
        conn.row_factory = sqlite3.Row
        try:
            if station_name:
                return conn.execute(
                    "SELECT * FROM all_passes WHERE station_name = ? ORDER BY pass_date, pass_start_time",
                    (station_name,),
                ).fetchall()
            return conn.execute(
                "SELECT * FROM all_passes ORDER BY pass_date, pass_start_time"
            ).fetchall()
        finally:
            conn.row_factory = prev_factory

    # Количество коммерческих пролётов, заказанных на день (опционально — только до указанного момента по UTC).
    def get_commercial_passes_planned_count(
        self,
        stat_day: date | datetime | str,
        up_to_datetime: Optional[datetime] = None,
        ) -> int:

        """Возвращает число коммерческих пролётов за день.

        Если задан up_to_datetime (UTC), считаются только пролёты с rx_start_time <= этого момента
        (уже «должны были начаться» к текущему времени).
        """
        day_value = self._normalize_date(stat_day)
        conn = self._connect()
        if up_to_datetime is not None:
            up_to_str = up_to_datetime.strftime("%Y-%m-%d %H:%M:%S")
            row = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM commercial_passes
                WHERE date(rx_start_time) = ? AND rx_start_time <= ?
                """,
                (day_value, up_to_str),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM commercial_passes
                WHERE date(rx_start_time) = ?
                """,
                (day_value,),
            ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    # Статистика коммерческих пролётов по станциям за день (для письма, как в old_GroundLinkServer).
    def get_commercial_passes_stats_by_station(
        self,
        stat_day: date | datetime | str,
        up_to_datetime: Optional[datetime] = None,
        ) -> Tuple[Dict[str, Dict[str, int]], Dict[str, int]]:
        """Возвращает (stats, totals) для блока «Коммерческие пролеты» в письме.

        stats = {station_name: {"planned": N, "successful": N, "not_received": N}}
        totals = {"planned": P, "successful": S, "not_received": R}
        Если задан up_to_datetime (UTC), учитываются только пролёты с rx_start_time <= этого момента.
        """
        day_value = self._normalize_date(stat_day)
        conn = self._connect()
        up_to_str = up_to_datetime.strftime("%Y-%m-%d %H:%M:%S") if up_to_datetime else None

        # Запланировано по станциям
        if up_to_str:
            planned_rows = conn.execute(
                """
                SELECT station_name, COUNT(*) AS cnt FROM commercial_passes
                WHERE date(rx_start_time) = ? AND rx_start_time <= ?
                GROUP BY station_name
                """,
                (day_value, up_to_str),
            ).fetchall()
        else:
            planned_rows = conn.execute(
                """
                SELECT station_name, COUNT(*) AS cnt FROM commercial_passes
                WHERE date(rx_start_time) = ?
                GROUP BY station_name
                """,
                (day_value,),
            ).fetchall()

        stats: Dict[str, Dict[str, int]] = {}
        totals = {"planned": 0, "successful": 0, "not_received": 0}

        for station_name, planned in planned_rows:
            planned = int(planned)
            stats[station_name] = {"planned": planned, "successful": 0, "not_received": planned}
            totals["planned"] += planned
            totals["not_received"] += planned

        # Принято по станциям (успешные в all_passes)
        if up_to_str:
            received_rows = conn.execute(
                """
                SELECT cp.station_name, COUNT(DISTINCT cp.id) AS cnt
                FROM commercial_passes cp
                INNER JOIN all_passes ap
                  ON ap.station_name = cp.station_name
                 AND ap.satellite_name = cp.satellite_name
                 AND ap.pass_date = date(cp.rx_start_time)
                 AND ap.success = 1
                WHERE date(cp.rx_start_time) = ? AND cp.rx_start_time <= ?
                GROUP BY cp.station_name
                """,
                (day_value, up_to_str),
            ).fetchall()
        else:
            received_rows = conn.execute(
                """
                SELECT cp.station_name, COUNT(DISTINCT cp.id) AS cnt
                FROM commercial_passes cp
                INNER JOIN all_passes ap
                  ON ap.station_name = cp.station_name
                 AND ap.satellite_name = cp.satellite_name
                 AND ap.pass_date = date(cp.rx_start_time)
                 AND ap.success = 1
                WHERE date(cp.rx_start_time) = ?
                GROUP BY cp.station_name
                """,
                (day_value,),
            ).fetchall()

        for station_name, received in received_rows:
            received = int(received)
            if station_name in stats:
                stats[station_name]["successful"] = received
                stats[station_name]["not_received"] = stats[station_name]["planned"] - received
            totals["successful"] += received
        totals["not_received"] = totals["planned"] - totals["successful"]

        return stats, totals

    # Статистика коммерческих пролётов по станциям за период (диапазон дат).
    def get_commercial_passes_stats_by_station_range(
        self,
        start_day: date | datetime | str,
        end_day: date | datetime | str,
        ) -> Tuple[Dict[str, Dict[str, int]], Dict[str, int]]:
        """Возвращает (stats, totals) за период дат.

        stats = {station_name: {"planned": N, "successful": N, "not_received": N}}
        totals = {"planned": P, "successful": S, "not_received": R}
        """
        start_value = self._normalize_date(start_day)
        end_value = self._normalize_date(end_day)
        conn = self._connect()

        planned_rows = conn.execute(
            """
            SELECT station_name, COUNT(*) AS cnt FROM commercial_passes
            WHERE date(rx_start_time) BETWEEN ? AND ?
            GROUP BY station_name
            """,
            (start_value, end_value),
        ).fetchall()

        stats: Dict[str, Dict[str, int]] = {}
        totals = {"planned": 0, "successful": 0, "not_received": 0}

        for station_name, planned in planned_rows:
            planned = int(planned)
            stats[station_name] = {"planned": planned, "successful": 0, "not_received": planned}
            totals["planned"] += planned
            totals["not_received"] += planned

        received_rows = conn.execute(
            """
            SELECT cp.station_name, COUNT(DISTINCT cp.id) AS cnt
            FROM commercial_passes cp
            INNER JOIN all_passes ap
              ON ap.station_name = cp.station_name
             AND ap.satellite_name = cp.satellite_name
             AND ap.pass_date = date(cp.rx_start_time)
             AND ap.success = 1
            WHERE date(cp.rx_start_time) BETWEEN ? AND ?
            GROUP BY cp.station_name
            """,
            (start_value, end_value),
        ).fetchall()

        for station_name, received in received_rows:
            received = int(received)
            if station_name in stats:
                stats[station_name]["successful"] = received
                stats[station_name]["not_received"] = stats[station_name]["planned"] - received
            totals["successful"] += received
        totals["not_received"] = totals["planned"] - totals["successful"]

        return stats, totals

    # Количество коммерческих пролётов, принятых за день (есть успешная запись в all_passes).
    def get_commercial_passes_received_count(
        self,
        stat_day: date | datetime | str,
        up_to_datetime: Optional[datetime] = None,
        ) -> int:
        """Возвращает число коммерческих пролётов за день, по которым есть успешный приём в all_passes.

        Сопоставление: станция + спутник + дата. Если задан up_to_datetime (UTC), считаются только
        пролёты с rx_start_time <= этого момента — тогда принято не может превысить заказано.
        """
        day_value = self._normalize_date(stat_day)
        conn = self._connect()
        if up_to_datetime is not None:
            up_to_str = up_to_datetime.strftime("%Y-%m-%d %H:%M:%S")
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT cp.id) AS cnt
                FROM commercial_passes cp
                INNER JOIN all_passes ap
                  ON ap.station_name = cp.station_name
                 AND ap.satellite_name = cp.satellite_name
                 AND ap.pass_date = date(cp.rx_start_time)
                 AND ap.success = 1
                WHERE date(cp.rx_start_time) = ? AND cp.rx_start_time <= ?
                """,
                (day_value, up_to_str),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT cp.id) AS cnt
                FROM commercial_passes cp
                INNER JOIN all_passes ap
                  ON ap.station_name = cp.station_name
                 AND ap.satellite_name = cp.satellite_name
                 AND ap.pass_date = date(cp.rx_start_time)
                 AND ap.success = 1
                WHERE date(cp.rx_start_time) = ?
                """,
                (day_value,),
            ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    # Список коммерческих пролётов за день без приёма, с ссылкой на график (если есть неуспешная запись в all_passes).
    def get_commercial_passes_not_received_list(
        self,
        stat_day: date | datetime | str,
        up_to_datetime: Optional[datetime] = None,
        ) -> List[Tuple[str, str, str, str, str]]:
        """Возвращает список (station_name, satellite_name, rx_start_time, rx_end_time, graph_url) для пролётов без приёма.
        graph_url — ссылка на график из all_passes (success=0), если есть; иначе пустая строка.
        """
        day_value = self._normalize_date(stat_day)
        conn = self._connect()
        subq = """
            (SELECT ap.graph_url FROM all_passes ap
             WHERE ap.station_name = cp.station_name
               AND ap.satellite_name = cp.satellite_name
               AND ap.pass_date = date(cp.rx_start_time)
               AND ap.success = 0
               AND ap.graph_url IS NOT NULL AND ap.graph_url != ''
             LIMIT 1)
        """
        if up_to_datetime is not None:
            up_to_str = up_to_datetime.strftime("%Y-%m-%d %H:%M:%S")
            rows = conn.execute(
                f"""
                SELECT cp.station_name, cp.satellite_name, cp.rx_start_time, cp.rx_end_time, {subq} AS graph_url
                FROM commercial_passes cp
                LEFT JOIN all_passes ap
                  ON ap.station_name = cp.station_name
                 AND ap.satellite_name = cp.satellite_name
                 AND ap.pass_date = date(cp.rx_start_time)
                 AND ap.success = 1
                WHERE date(cp.rx_start_time) = ? AND cp.rx_start_time <= ? AND ap.id IS NULL
                ORDER BY cp.station_name, cp.rx_start_time
                """,
                (day_value, up_to_str),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT cp.station_name, cp.satellite_name, cp.rx_start_time, cp.rx_end_time, {subq} AS graph_url
                FROM commercial_passes cp
                LEFT JOIN all_passes ap
                  ON ap.station_name = cp.station_name
                 AND ap.satellite_name = cp.satellite_name
                 AND ap.pass_date = date(cp.rx_start_time)
                 AND ap.success = 1
                WHERE date(cp.rx_start_time) = ? AND ap.id IS NULL
                ORDER BY cp.station_name, cp.rx_start_time
                """,
                (day_value,),
            ).fetchall()
        return [
            (str(r[0]), str(r[1]), str(r[2] or ""), str(r[3] or ""), str(r[4] or "").strip())
            for r in rows
        ]

    # Список коммерческих пролётов за период без приёма.
    def get_commercial_passes_not_received_list_range(
        self,
        start_day: date | datetime | str,
        end_day: date | datetime | str,
        ) -> List[Tuple[str, str, str, str, str]]:
        """Возвращает список (station_name, satellite_name, rx_start_time, rx_end_time, graph_url) за период."""
        start_value = self._normalize_date(start_day)
        end_value = self._normalize_date(end_day)
        conn = self._connect()
        subq = """
            (SELECT ap.graph_url FROM all_passes ap
             WHERE ap.station_name = cp.station_name
               AND ap.satellite_name = cp.satellite_name
               AND ap.pass_date = date(cp.rx_start_time)
               AND ap.success = 0
               AND ap.graph_url IS NOT NULL AND ap.graph_url != ''
             LIMIT 1)
        """
        rows = conn.execute(
            f"""
            SELECT cp.station_name, cp.satellite_name, cp.rx_start_time, cp.rx_end_time, {subq} AS graph_url
            FROM commercial_passes cp
            LEFT JOIN all_passes ap
              ON ap.station_name = cp.station_name
             AND ap.satellite_name = cp.satellite_name
             AND ap.pass_date = date(cp.rx_start_time)
             AND ap.success = 1
            WHERE date(cp.rx_start_time) BETWEEN ? AND ? AND ap.id IS NULL
            ORDER BY cp.station_name, cp.rx_start_time
            """,
            (start_value, end_value),
        ).fetchall()
        return [
            (str(r[0]), str(r[1]), str(r[2] or ""), str(r[3] or ""), str(r[4] or "").strip())
            for r in rows
        ]

    # Возвращает статистику успешных пролетов за указанный день.
    def get_daily_success_stats(self, stat_day: date | datetime | str) -> list[list]:
        """Возвращает статистику успешных пролетов за указанный день."""
        day_value = self._normalize_date(stat_day)
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT station_name, total_passes, success_passes, failed_passes, failed_percent
            FROM station_stats
            WHERE stat_day = ?
            ORDER BY station_name
            """,
            (day_value,),
        ).fetchall()
        if not rows:
            return []
        result: list[list] = []
        total_all = 0
        success_all = 0
        failed_all = 0
        for station_name, total, success, failed, failed_percent in rows:
            total_all += total
            success_all += success
            failed_all += failed
            result.append([station_name, total, success, failed, failed_percent])
        failed_percent_all = round((failed_all * 100.0) / total_all, 2) if total_all else 0.0
        result.append(["total", total_all, success_all, failed_all, failed_percent_all])
        return result

    # Возвращает статистику по станциям за день с средним SNR.
    def get_daily_station_stats(self, stat_day: date | datetime | str) -> list[list]:
        """Возвращает статистику по станциям за день с средним SNR."""
        day_value = self._normalize_date(stat_day)
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT s.station_name,
                   s.total_passes,
                   s.success_passes,
                   s.failed_passes,
                   s.failed_percent,
                   ROUND(AVG(p.snr_awg), 2) AS snr_awg
            FROM station_stats s
            LEFT JOIN all_passes p
              ON p.station_name = s.station_name
             AND p.pass_date = s.stat_day
            WHERE s.stat_day = ?
            GROUP BY s.station_name, s.total_passes, s.success_passes, s.failed_passes, s.failed_percent
            ORDER BY s.station_name
            """,
            (day_value,),
        ).fetchall()
        result = []
        for station_name, total, success, failed, failed_percent, snr_awg in rows:
            result.append([station_name, total, success, failed, failed_percent, snr_awg or 0.0])
        return result

    def get_range_station_stats(
        self,
        start_day: date | datetime | str,
        end_day: date | datetime | str,
        ) -> list[list]:
        """Возвращает статистику по станциям за диапазон дат (средний SNR по всем логам)."""
        start_value = self._normalize_date(start_day)
        end_value = self._normalize_date(end_day)
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT station_name,
                   SUM(total_passes) AS total_passes,
                   SUM(success_passes) AS success_passes,
                   SUM(failed_passes) AS failed_passes
            FROM station_stats
            WHERE stat_day BETWEEN ? AND ?
            GROUP BY station_name
            ORDER BY station_name
            """,
            (start_value, end_value),
        ).fetchall()

        snr_rows = conn.execute(
            """
            SELECT station_name, AVG(snr_awg) AS snr_awg
            FROM all_passes
            WHERE pass_date BETWEEN ? AND ?
              AND snr_awg IS NOT NULL
            GROUP BY station_name
            """,
            (start_value, end_value),
        ).fetchall()
        snr_map = {str(name): snr_awg for name, snr_awg in snr_rows}

        result: list[list] = []
        for station_name, total, success, failed in rows:
            total_i = int(total or 0)
            failed_i = int(failed or 0)
            failed_percent = round((failed_i * 100.0) / total_i, 2) if total_i else 0.0
            result.append(
                [
                    str(station_name),
                    total_i,
                    int(success or 0),
                    failed_i,
                    failed_percent,
                    snr_map.get(str(station_name)) or 0.0,
                ]
            )
        return result

    # Возвращает по одному пролету с максимальной суммой SNR на станцию за день.
    def get_max_snr_sum_passes(self, stat_day: date | datetime | str) -> list[SatPas]:
        """Возвращает по одному пролету с максимальной суммой SNR на станцию за день."""
        day_value = self._normalize_date(stat_day)
        conn = self._connect()
        prev_factory = conn.row_factory
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT p.*
                FROM all_passes p
                JOIN (
                    SELECT station_name, MAX(snr_sum) AS max_snr_sum
                    FROM all_passes
                    WHERE pass_date = ?
                      AND snr_sum IS NOT NULL
                    GROUP BY station_name
                ) mx
                  ON p.station_name = mx.station_name
                 AND p.snr_sum = mx.max_snr_sum
                WHERE p.pass_date = ?
                ORDER BY p.station_name, p.pass_start_time
                """,
                (day_value, day_value),
            ).fetchall()
        finally:
            conn.row_factory = prev_factory
        result = []
        for row in rows:
            sat_pass = SatPas(
                station_name=row["station_name"],
                satellite_name=row["satellite_name"],
                pass_date=self._parse_date(row["pass_date"]),
                pass_start_time=row["pass_start_time"],
                pass_end_time=row["pass_end_time"],
                rx_start_time=row["rx_start_time"],
                rx_end_time=row["rx_end_time"],
                snr_sum=row["snr_sum"],
                log_url=row["log_url"],
                log_path=row["log_path"],
                graph_url=row["graph_url"],
                graph_path=row["graph_path"],
                success=bool(row["success"]),
            )
            result.append(sat_pass)
        return result

    # Возвращает ссылки на графики пустых пролетов по станциям за день.
    def get_failed_graphs_by_station(self, stat_day: date | datetime | str) -> dict[str, list[str]]:
        """Возвращает графики пустых пролетов по станциям за день."""
        day_value = self._normalize_date(stat_day)
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT station_name, graph_url
            FROM all_passes
            WHERE pass_date = ?
              AND success = 0
              AND graph_url IS NOT NULL
              AND graph_url != ''
            ORDER BY station_name, pass_start_time
            """,
            (day_value,),
        ).fetchall()
        result: dict[str, list[str]] = {}
        for station_name, graph_url in rows:
            result.setdefault(station_name, []).append(graph_url)
        return result


if __name__ == "__main__":
    # Простой тестовый сценарий, аналогичный примерам в EusLogDownloader.py
    # Используем основную БД проекта.
    BASE_DIR = Path("/root/lorett/GroundLinkServer")
    logger = Logger(path_log="db_manager", log_level="info")
    db = DbManager(logger=logger)

    # Создаем станцию
    station = "TEST_STATION"
    now = datetime.now()

    # Очистка тестовых данных, если они уже есть.
    conn = db._connect()
    conn.execute("DELETE FROM all_passes WHERE station_name = ?", (station,))
    conn.execute("DELETE FROM commercial_passes WHERE station_name = ?", (station,))
    conn.execute("DELETE FROM station_stats WHERE station_name = ?", (station,))
    conn.commit()

    # Создаем первый пролет
    pass_1 = SatPas(
        station_name=station,
        satellite_name="SAT-1",
        pass_date=now.date(),
        pass_start_time=now,
        pass_end_time=now,
        rx_start_time=now,
        rx_end_time=now,
        log_url="http://example.com/logs/SAT-1.log",
        log_path=str(BASE_DIR / "logs" / "SAT-1.log"),
        graph_url="http://example.com/graphs/SAT-1.png",
        graph_path=str(BASE_DIR / "graphs" / "SAT-1.png"),
        success=True,
    )

    # Создаем второй пролет
    pass_2 = SatPas(
        station_name=station,
        satellite_name="SAT-2",
        pass_date=now.date(),
        pass_start_time=now,
        pass_end_time=now,
        rx_start_time=now,
        rx_end_time=now,
        log_url="http://example.com/logs/SAT-2.log",
        log_path=str(BASE_DIR / "logs" / "SAT-2.log"),
        graph_url="http://example.com/graphs/SAT-2.png",
        graph_path=str(BASE_DIR / "graphs" / "SAT-2.png"),
        success=False,
    )
    # Добавляем пролета в базу данных
    pass_id_1 = db.add_pass(pass_1, is_commercial=False)
    pass_id_2 = db.add_pass(pass_2, is_commercial=True)
    # Добавляем коммерческий пролет в базу данных
    commercial_id = db.add_commercial_pass(
        station_name=station,
        satellite_name="SAT-2",
        pass_type="комерческий",
        comment="тестовый заказ",
        rx_start_time=now,
        rx_end_time=now,
    )

    # Возвращаем список пролетов из базы данных
    passes = db.list_passes(station_name=station)
    # Проверяем, что список пролетов содержит 2 пролета
    assert len(passes) == 2, "expected 2 passes"
    assert all(p["id"] in (pass_id_1, pass_id_2) for p in passes), "pass ids mismatch"

    # Проверяем статистику успешных пролетов за указанный день
    conn = db._connect()
    row = conn.execute(
        """
        SELECT total_passes, success_passes, failed_passes, failed_percent
        FROM station_stats
        WHERE station_name = ?
          AND stat_day = ?
        """,
        (station, now.date().isoformat()),
    ).fetchone()

    assert row is not None, "stats row not found" # проверяем, что строка статистики найдена
    total_passes, success_passes, failed_passes, failed_percent = row
    assert total_passes == 2, "total_passes should be 2" # проверяем, что общее количество пролетов равно 2
    assert success_passes == 1, "success_passes should be 1" # проверяем, что количество успешных пролетов равно 1
    assert failed_passes == 1, "failed_passes should be 1" # проверяем, что количество неудачных пролетов равно 1
    assert round(float(failed_percent), 2) == 50.0, "failed_percent should be 50" # проверяем, что процент неудачных пролетов равен 50% 
    assert commercial_id > 0, "commercial pass insert failed" # проверяем, что ID коммерческого пролета больше 0

    print("DbManager tests passed") # выводим сообщение о прохождении тестов
