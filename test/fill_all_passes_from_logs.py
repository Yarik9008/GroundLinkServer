import sqlite3
from pathlib import Path
from typing import Optional, Tuple

DB_PATH = "/root/lorett/GroundLinkMonitorServer/all_passes.db"
LOGS_DIR = Path("/root/lorett/GroundLinkMonitorServer/logs")
REPORT_PATH = Path("/root/lorett/GroundLinkMonitorServer/all_passes_skipped_report.txt")
X_BEND_FAILURE_THRESHOLD = 3.85
L_BEND_FAILURE_THRESHOLD = 0.0


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
            session_end TIMESTAMP NOT NULL,
            successful TEXT NOT NULL CHECK (successful IN ('Yes', 'No'))
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
    successful: str,
) -> None:
    ensure_station_table(conn, station)
    table_name = quote_identifier(station)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT id, successful
        FROM {table_name}
        WHERE satellite = ?
          AND session_start = ?
          AND session_end = ?
        LIMIT 1
        """,
        (satellite, session_start, session_end),
    )
    existing = cursor.fetchone()
    if existing is not None:
        row_id, current_successful = existing
        if current_successful != successful:
            cursor.execute(
                f"UPDATE {table_name} SET successful = ? WHERE id = ?",
                (successful, row_id),
            )
            conn.commit()
        cursor.close()
        return
    cursor.execute(
        f"""
        INSERT INTO {table_name} (satellite, session_start, session_end, successful)
        VALUES (?, ?, ?, ?)
        """,
        (satellite, session_start, session_end, successful),
    )
    conn.commit()
    cursor.close()


def _normalize_data_time(value: str) -> Optional[str]:
    value = value.strip()
    if not value:
        return None
    # Отбрасываем миллисекунды, если есть.
    if "." in value:
        value = value.split(".", 1)[0]
    return value


def detect_bend_type_from_header(log_file_path: Path) -> str:
    try:
        with log_file_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if line.startswith("#Time"):
                    parts = line.split("\t")
                    if "Level2" in line or len(parts) > 6:
                        return "X"
                    return "L"
        return "L"
    except OSError:
        return "L"


def extract_snr_from_log(log_file_path: Path, bend_type: Optional[str] = None) -> Tuple[float, int]:
    snr_sum = 0.0
    snr_count = 0
    if bend_type is None:
        bend_type = detect_bend_type_from_header(log_file_path)
    snr_column_index = 5 if bend_type == "X" else 4
    try:
        with log_file_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if not line.strip() or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) > snr_column_index:
                    try:
                        snr_sum += float(parts[snr_column_index])
                        snr_count += 1
                    except (ValueError, IndexError):
                        continue
    except OSError:
        return 0.0, 0
    return snr_sum, snr_count


def evaluate_successful(log_path: Path) -> str:
    bend_type = detect_bend_type_from_header(log_path)
    threshold = X_BEND_FAILURE_THRESHOLD if bend_type == "X" else L_BEND_FAILURE_THRESHOLD
    snr_sum, snr_count = extract_snr_from_log(log_path, bend_type)
    avg_snr = snr_sum / snr_count if snr_count > 0 else 0.0
    return "Yes" if avg_snr > threshold else "No"


def parse_log_metadata(
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
                    start_time = _normalize_data_time(line.split(":", 1)[1])
                    continue
                if line[0].isdigit():
                    # Берем время из последней строки данных.
                    parts = line.split()
                    if len(parts) >= 2 and ":" in parts[1]:
                        data_time = f"{parts[0]} {parts[1]}"
                    else:
                        data_time = parts[0]
                    data_time = _normalize_data_time(data_time)
                    if data_time:
                        if first_data_time is None:
                            first_data_time = data_time
                        last_data_time = data_time
    except OSError:
        return None, "read_error"

    station = header_station or station
    session_start = start_time or first_data_time
    session_end = last_data_time

    missing = []
    if not satellite:
        missing.append("missing_satellite")
    if not session_start:
        missing.append("missing_start_time")
    if not session_end:
        missing.append("missing_end_time")
    if missing:
        return None, ",".join(missing)

    successful = evaluate_successful(log_path)
    return (station, satellite, session_start, session_end, successful), None


def fill_from_logs(conn: sqlite3.Connection, logs_dir: Path) -> None:
    total = 0
    skipped = 0
    skipped_entries: list[str] = []
    entries: list[tuple[str, str, str, str, str]] = []
    for log_path in logs_dir.rglob("*.log"):
        metadata, reason = parse_log_metadata(log_path)
        if metadata is None:
            skipped += 1
            skipped_entries.append(f"{log_path}\t{reason or 'unknown'}")
            continue
        entries.append(metadata)
    entries.sort(key=lambda item: item[2])
    for station, satellite, session_start, session_end, successful in entries:
        add_pass(conn, station, satellite, session_start, session_end, successful)
        total += 1
    print(f"Готово: обработано логов {total}, пропущено {skipped}")
    if skipped_entries:
        REPORT_PATH.write_text("\n".join(skipped_entries) + "\n", encoding="utf-8")
        print(f"Отчет по пропущенным файлам: {REPORT_PATH}")


def main() -> None:
    conn = init_db(DB_PATH)
    fill_from_logs(conn, LOGS_DIR)
    conn.close()


if __name__ == "__main__":
    main()
