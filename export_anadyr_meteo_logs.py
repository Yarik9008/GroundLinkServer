import argparse
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple


def _load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    try:
        return json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _normalize_list(values: Iterable[str]) -> List[str]:
    return [str(v).strip() for v in values if str(v).strip()]


def _load_satellite_lists(config: dict) -> Tuple[Set[str], Set[str]]:
    commercial = _normalize_list(config.get("commercial_satellites", []) or [])
    meteorological = _normalize_list(config.get("meteorological_satellites", []) or [])
    return {s.upper() for s in commercial}, {s.upper() for s in meteorological}


def _resolve_out_dir(base_dir: Path, out_dir: str) -> Path:
    target = Path(out_dir)
    if not target.is_absolute():
        target = base_dir / target
    target.mkdir(parents=True, exist_ok=True)
    return target


def _query_pass_logs(
    conn: sqlite3.Connection,
    station_name: str,
    min_snr: float,
) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT id, station_name, satellite_name, pass_date, pass_start_time,
               snr_awg, log_path
        FROM all_passes
        WHERE station_name = ?
          AND success = 1
          AND snr_awg > ?
          AND log_path IS NOT NULL
          AND log_path != ''
        ORDER BY pass_date, pass_start_time
        """,
        (station_name, min_snr),
    ).fetchall()


def _should_include_satellite(
    satellite_name: str,
    meteorological_set: Set[str],
    commercial_set: Set[str],
    require_meteorological_list: bool,
) -> bool:
    sat_norm = str(satellite_name or "").strip().upper()
    if not sat_norm:
        return False
    if sat_norm in commercial_set:
        return False
    if meteorological_set:
        return sat_norm in meteorological_set
    return not require_meteorological_list


def _copy_log(src: Path, dest_dir: Path, pass_id: int, dry_run: bool) -> Optional[Path]:
    if not src.exists() or not src.is_file():
        return None
    dest = dest_dir / src.name
    if dest.exists():
        stem = dest.stem
        suffix = dest.suffix
        dest = dest_dir / f"{stem}_{pass_id}{suffix}"
    if dry_run:
        return dest
    shutil.copy2(src, dest)
    return dest


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Экспорт логов успешных метеорологических пролётов со станции R4.6S_Anadyr."
    )
    parser.add_argument("--db", default=str(base_dir / "groundlink.db"), help="Путь к SQLite базе")
    parser.add_argument("--config", default=str(base_dir / "config.json"), help="Путь к config.json")
    parser.add_argument("--station", default="R4.6S_Anadyr", help="Название станции")
    parser.add_argument("--out-dir", default="AN_log", help="Каталог для копирования логов")
    parser.add_argument("--min-snr", type=float, default=12.0, help="Минимальный средний SNR")
    parser.add_argument("--dry-run", action="store_true", help="Только показать, без копирования")
    args = parser.parse_args()

    config = _load_config(Path(args.config))
    commercial_set, meteorological_set = _load_satellite_lists(config)
    require_meteorological_list = False
    if not meteorological_set:
        require_meteorological_list = False
        print(
            "В config.json нет списка meteorological_satellites. "
            "Будут использованы все спутники, кроме commercial_satellites."
        )

    out_dir = _resolve_out_dir(base_dir, args.out_dir)

    conn = sqlite3.connect(args.db)
    try:
        rows = _query_pass_logs(conn, args.station, args.min_snr)
    finally:
        conn.close()

    total = 0
    copied = 0
    skipped_missing = 0
    skipped_sat = 0

    for row in rows:
        total += 1
        satellite_name = row["satellite_name"]
        if not _should_include_satellite(
            satellite_name,
            meteorological_set,
            commercial_set,
            require_meteorological_list=require_meteorological_list,
        ):
            skipped_sat += 1
            continue
        log_path = Path(str(row["log_path"]))
        dest = _copy_log(log_path, out_dir, int(row["id"]), args.dry_run)
        if dest is None:
            skipped_missing += 1
            continue
        copied += 1

    print(f"Найдено пролётов: {total}")
    print(f"Скопировано логов: {copied}")
    if skipped_sat:
        print(f"Исключено по спутникам: {skipped_sat}")
    if skipped_missing:
        print(f"Не найдено файлов логов: {skipped_missing}")
    print(f"Каталог результата: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
