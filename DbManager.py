import sqlite3
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterable, Optional
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
        - list_passes: возвращает список пролетов с фильтром по станции.
        - get_daily_success_stats: статистика успешности за день.
        - get_daily_station_stats: статистика по станциям за день.
        - get_max_snr_sum_passes: пролет с max snr_sum по станции за день.
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
        self._init_schema()

    # Создает каталог для файла базы данных, если он не существует.
    def _ensure_parent_dir(self) -> None:
        """Создает каталог для файла базы данных."""
        parent = Path(self.db_path).parent
        parent.mkdir(parents=True, exist_ok=True)

    # Открывает соединение с SQLite и включает foreign_keys.
    def _connect(self) -> sqlite3.Connection:
        """Открывает соединение с SQLite и включает foreign_keys."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    # Создает таблицы и индексы при отсутствии.
    def _init_schema(self) -> None:
        """Создает таблицы и индексы при отсутствии."""
        with self._connect() as conn:
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
        with self._connect() as conn:
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
                    sat_pass.snr_awg,
                    sat_pass.snr_max,
                    sat_pass.snr_sum,
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
            return int(cur.lastrowid)

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
                (failed_inc * 100.0) / total_inc if total_inc else 0.0,
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
        with self._connect() as conn:
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
            return int(cur.lastrowid)

    # Возвращает список пролетов (опционально по станции).
    def list_passes(self, station_name: Optional[str] = None) -> Iterable[sqlite3.Row]:
        """Возвращает список пролетов (опционально по станции).

        Args:
            station_name: Название станции для фильтрации.

        Returns:
            Iterable[sqlite3.Row]: Список строк из all_passes.
        """
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            if station_name:
                return conn.execute(
                    "SELECT * FROM all_passes WHERE station_name = ? ORDER BY pass_date, pass_start_time",
                    (station_name,),
                ).fetchall()
            return conn.execute(
                "SELECT * FROM all_passes ORDER BY pass_date, pass_start_time"
            ).fetchall()

    # Возвращает статистику успешных пролетов за указанный день.
    def get_daily_success_stats(self, stat_day: date | datetime | str) -> list[list]:
        """Возвращает статистику успешных пролетов за указанный день."""
        day_value = self._normalize_date(stat_day)
        with self._connect() as conn:
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
        with self._connect() as conn:
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

    # Возвращает по одному пролету с максимальной суммой SNR на станцию за день.
    def get_max_snr_sum_passes(self, stat_day: date | datetime | str) -> list[SatPas]:
        """Возвращает по одному пролету с максимальной суммой SNR на станцию за день."""
        day_value = self._normalize_date(stat_day)
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
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
        result = []
        for row in rows:
            sat_pass = SatPas(
                station_name=row["station_name"],
                satellite_name=row["satellite_name"],
                pass_date=row["pass_date"],
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



if __name__ == "__main__":
    # Простой тестовый сценарий, аналогичный примерам в EusLogDownloader.py
    # Используем основную БД проекта.
    logger = Logger(path_log="db_manager", log_level="info")
    db = DbManager(logger=logger)

    # Создаем станцию
    station = "TEST_STATION"
    now = datetime.now()

    # Очистка тестовых данных, если они уже есть.
    with db._connect() as conn:
        conn.execute("DELETE FROM all_passes WHERE station_name = ?", (station,))
        conn.execute("DELETE FROM commercial_passes WHERE station_name = ?", (station,))
        conn.execute("DELETE FROM station_stats WHERE station_name = ?", (station,))

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
        log_path=r"C:\logs\SAT-1.log",
        graph_url="http://example.com/graphs/SAT-1.png",
        graph_path=r"C:\graphs\SAT-1.png",
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
        log_path=r"C:\logs\SAT-2.log",
        graph_url="http://example.com/graphs/SAT-2.png",
        graph_path=r"C:\graphs\SAT-2.png",
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
    with db._connect() as conn:
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
