"""GroundLinkServer — основной сервер мониторинга наземных станций приёма.

Оркестрирует загрузку логов с портала EUS, анализ пролётов, синхронизацию
коммерческих пролётов из Telegram, формирование статистики и отправку отчётов.
Запуск: python GroundLinkServer.py [start_date] [end_date] [--sch] [--off-email] [--debag-email].
"""

import argparse
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from Logger import Logger
from GraphGenerator import GraphGenerator
from EusLogDownloader import EusLogDownloader
from DbManager import DbManager
from PassAnalyzer import PassAnalyzer
from SatPass import SatPas
from EmailClient import EmailClient
from TelClient import TelClient
from colorama import Fore, Style, init as colorama_init
colorama_init(autoreset=True)


class GroundLinkServer:
    """Основной сервер: загрузка логов, анализ, БД, статистика, email.

    Использует EusLogDownloader, PassAnalyzer, DbManager, TelClient, EmailClient,
    GraphGenerator. Конфигурация — config.json в каталоге скрипта.

    Методы:
        __init__: Инициализация логгеров, БД, загрузчика, анализатора, email, графиков.
        _load_config: Загрузка config.json.
        buily_daily_pass_stats: Сбор статистики пролётов за день.
        print_log_day_stats: Вывод статистики пролётов за день в консоль.
        build_range_pass_stats: Сбор статистики пролётов за период.
        print_log_period_stats: Вывод статистики пролётов за период.
        build_week_pass_stats, print_log_week_stats: Статистика за 7 дней.
        build_month_pass_stats, print_log_month_stats: Статистика за месяц.
        build_comm_day_stats, print_comm_day_stats: Коммерческие пролёты за день.
        build_comm_period_stats, print_comm_period_stats: Коммерческие за период.
        build_comm_week_stats, print_comm_week_stats: Коммерческие за 7 дней.
        build_comm_month_stats, print_comm_month_stats: Коммерческие за месяц.
        main: Основной цикл: sync Telegram → backfill → загрузка → статистика → email.
        _download_and_analyze_range: Загрузка логов, анализ, запись в БД.

    Атрибуты:
        logger, config, db_manager, eus, analyzer, email_client, graph_generator.
    """

    # Инициализация сервера: логгеры, конфиг, БД, загрузчик, анализатор, email, графики.
    def __init__(self, path_log: str) -> None:
        """Инициализирует сервер и все компоненты.

        Args:
            path_log: Путь к каталогу логов (с завершающим разделителем).
        """
        self.logger = Logger(path_log=path_log, log_level="info", logger_name="MAIN")

        # загрузка конфига
        self.config = self._load_config(path_log)

        # инициализация обработчика базы данных
        self.logger_db = Logger(path_log=path_log, log_level="info", logger_name="DB")
        self.db_manager = DbManager(logger=self.logger_db)

        # uнициализация загрузчика лог файлов
        self.logger_eus = Logger(path_log=path_log, log_level="info", logger_name="EUS")
        self.eus = EusLogDownloader(logger=self.logger_eus)

        # инициализация анализатора логов
        self.logger_analyzer = Logger(path_log=path_log, log_level="info", logger_name="ANALYZER")
        self.analyzer = PassAnalyzer(logger=self.logger_analyzer)

        # инициализация клиента email (конфиг передаётся при создании)
        self.logger_email = Logger(path_log=path_log, log_level="info", logger_name="EMAIL")
        self.email_client = EmailClient(logger=self.logger_email, config=self.config)

        # генератор графиков (использует db_manager для данных)
        self.graph_generator = GraphGenerator(db_manager=self.db_manager, logger=self.logger)

    # Загрузка config.json из каталога скрипта.
    def _load_config(self, path_log: str) -> dict:
        """Загружает config.json из каталога скрипта.

        Returns:
            dict: Конфигурация или {} при ошибке.
        """
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, "config.json")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.info(f"config not found: {config_path}")
            return {}
        except (json.JSONDecodeError, OSError) as e:
            self.logger.warning(f"config load failed: {e}")
            return {}


    # Сбор статистики пролётов за день (все станции, пустые, итоги).
    def buily_daily_pass_stats(self, day):
        """Собирает статистику пролётов за день (rows, totals, max_passes, failed_graphs).

        Args:
            day: Дата (date или datetime).

        Returns:
            dict с date_display, rows, totals, max_passes, failed_graphs или None.
        """
        stats = self.db_manager.get_daily_station_stats(day)
        
        if not stats:
            return None

        if isinstance(day, datetime):
            date_display = day.date().isoformat()
        elif hasattr(day, "isoformat"):
            date_display = day.isoformat()
        else:
            date_display = str(day)

        total_files = 0
        total_success = 0
        total_failed = 0
        avg_values = []
        rows = []
        failed_graphs = []
        max_passes = []
        overall_avg = 0
        total_failed_percent = 0

        for station_name, total, success, failed, failed_percent, snr_awg in stats:
            total_files += int(total or 0)
            total_success += int(success or 0)
            total_failed += int(failed or 0)
            if total and snr_awg is not None:
                avg_values.append(float(snr_awg))
            rows.append((station_name, total, success, failed, failed_percent, snr_awg))

        total_failed_percent = (total_failed * 100.0 / total_files) if total_files else 0.0
        overall_avg = (sum(avg_values) / len(avg_values)) if avg_values else 0.0
        max_passes = self.db_manager.get_max_snr_sum_passes(day)
        failed_graphs = self.db_manager.get_failed_graphs_by_station(day)

        return {
            "date_display": date_display,
            "rows": rows,
            "totals": {
                "total_files": total_files,
                "total_success": total_success,
                "total_failed": total_failed,
                "total_failed_percent": total_failed_percent,
                "overall_avg": overall_avg,
            },
            "max_passes": max_passes,
            "failed_graphs": failed_graphs,
        }

    # Вывод статистики пролётов за день в консоль.
    def print_log_day_stats(self, stats) -> None:
        """Печатает статистику успешных пролетов за день."""
        if not stats:
            self.logger.info("no daily stats found")
            return
        date_display = stats["date_display"]

        print(f"\n{Fore.CYAN + Style.BRIGHT}ИТОГОВАЯ СВОДКА ПО ВСЕМ СТАНЦИЯМ  {date_display}")
        print(f"{Fore.CYAN}{'Станция':<30} {'Всего':>10} {'Успешных':>12} {'Пустых':>14} {'% пустых':>15} {'Средний SNR':>15}")
        print(f"{Fore.CYAN}{'-' * 110}")

        rows_sorted = sorted(stats["rows"], key=lambda r: float(r[4] or 0))
        for station_name, total, success, failed, failed_percent, snr_awg in rows_sorted:
            row_color = Fore.CYAN
            if float(failed_percent) > 25:
                row_color = Fore.RED
            elif float(failed_percent) > 5:
                row_color = Fore.YELLOW
            else:
                row_color = Fore.GREEN
            print(
                f"{row_color}{station_name:<30} {int(total):>10} {int(success):>12} "
                f"{int(failed):>14} {float(failed_percent):>14.1f}% {float(snr_awg or 0):>15.2f}"
            )

        totals = stats["totals"]
        print(f"{Fore.CYAN}{'-' * 110}")
        print(
            f"{Fore.GREEN + Style.BRIGHT}{'ИТОГО':<30} {totals['total_files']:>10} {totals['total_success']:>12} "
            f"{totals['total_failed']:>14} {totals['total_failed_percent']:>14.1f}% {totals['overall_avg']:>15.2f}"
        )

        # Пустые пролеты по станциям (ссылки на графики)
        failed_graphs = stats.get("failed_graphs") or {}
        if failed_graphs:
            print(f"\n{Fore.CYAN + Style.BRIGHT}ПУСТЫЕ ПРОЛЕТЫ ПО СТАНЦИЯМ (ГРАФИКИ)")
            for station_name in sorted(failed_graphs.keys()):
                print(f"{Fore.CYAN}{station_name}:")
                for graph_url in failed_graphs[station_name]:
                    print(f"  {graph_url}")

    # Сбор статистики пролётов за период дат.
    def build_range_pass_stats(self, start_day, end_day) -> dict:
        """Собирает агрегированную статистику пролётов за диапазон дат."""
        stats = self.db_manager.get_range_station_stats(start_day, end_day)

        if not stats:
            return {}

        if isinstance(start_day, datetime):
            start_display = start_day.date().isoformat()
        elif hasattr(start_day, "isoformat"):
            start_display = start_day.isoformat()
        else:
            start_display = str(start_day)

        if isinstance(end_day, datetime):
            end_display = end_day.date().isoformat()
        elif hasattr(end_day, "isoformat"):
            end_display = end_day.isoformat()
        else:
            end_display = str(end_day)

        total_files = 0
        total_success = 0
        total_failed = 0
        avg_values = []
        rows = []

        for station_name, total, success, failed, failed_percent, snr_awg in stats:
            total_files += int(total or 0)
            total_success += int(success or 0)
            total_failed += int(failed or 0)
            if total and snr_awg is not None:
                avg_values.append(float(snr_awg))
            rows.append((station_name, total, success, failed, failed_percent, snr_awg))

        total_failed_percent = (total_failed * 100.0 / total_files) if total_files else 0.0
        overall_avg = (sum(avg_values) / len(avg_values)) if avg_values else 0.0

        return {
            "period_display": f"{start_display} — {end_display}",
            "rows": rows,
            "totals": {
                "total_files": total_files,
                "total_success": total_success,
                "total_failed": total_failed,
                "total_failed_percent": total_failed_percent,
                "overall_avg": overall_avg,
            },
        }

    # Вывод статистики пролётов за период в консоль.
    def print_log_period_stats(self, stats, title: str) -> None:
        """Печатает статистику успешных пролетов за период."""
        if not stats:
            self.logger.info("no period stats found")
            return
        period_display = stats["period_display"]

        print(f"\n{Fore.CYAN + Style.BRIGHT}{title}  {period_display}")
        print(f"{Fore.CYAN}{'Станция':<30} {'Всего':>10} {'Успешных':>12} {'Пустых':>14} {'% пустых':>15} {'Средний SNR':>15}")
        print(f"{Fore.CYAN}{'-' * 110}")

        rows_sorted = sorted(stats["rows"], key=lambda r: float(r[4] or 0))
        for station_name, total, success, failed, failed_percent, snr_awg in rows_sorted:
            row_color = Fore.CYAN
            if float(failed_percent) > 25:
                row_color = Fore.RED
            elif float(failed_percent) > 5:
                row_color = Fore.YELLOW
            else:
                row_color = Fore.GREEN
            print(
                f"{row_color}{station_name:<30} {int(total):>10} {int(success):>12} "
                f"{int(failed):>14} {float(failed_percent):>14.1f}% {float(snr_awg or 0):>15.2f}"
            )

        totals = stats["totals"]
        print(f"{Fore.CYAN}{'-' * 110}")
        print(
            f"{Fore.GREEN + Style.BRIGHT}{'ИТОГО':<30} {totals['total_files']:>10} {totals['total_success']:>12} "
            f"{totals['total_failed']:>14} {totals['total_failed_percent']:>14.1f}% {totals['overall_avg']:>15.2f}"
        )

    # Сбор статистики пролётов за 7 дней (end_day - 6 .. end_day).
    def build_week_pass_stats(self, end_day) -> dict:
        if isinstance(end_day, datetime):
            end_day = end_day.date()
        start_day = end_day - timedelta(days=6)
        return self.build_range_pass_stats(start_day, end_day)

    # Вывод статистики пролётов за неделю.
    def print_log_week_stats(self, stats) -> None:
        self.print_log_period_stats(stats, "ИТОГОВАЯ СВОДКА ПО СТАНЦИЯМ ЗА НЕДЕЛЮ")

    # Сбор статистики пролётов за месяц (с 1-го по end_day).
    def build_month_pass_stats(self, end_day) -> dict:
        if isinstance(end_day, datetime):
            end_day = end_day.date()
        start_day = end_day.replace(day=1)
        return self.build_range_pass_stats(start_day, end_day)

    # Вывод статистики пролётов за месяц.
    def print_log_month_stats(self, stats) -> None:
        self.print_log_period_stats(stats, "ИТОГОВАЯ СВОДКА ПО СТАНЦИЯМ ЗА МЕСЯЦ")

    # Сбор статистики коммерческих пролётов за день.
    def build_comm_day_stats(self, day, up_to_datetime=None) -> dict:
        """Собирает статистику коммерческих пролётов за день (rows, totals, not_received_list).

        Args:
            day: Дата.
            up_to_datetime: Для «сегодня» — учитывать только пролёты до этого момента.
        """
        now_utc = datetime.now(timezone.utc)
        # Если считаем статистику за сегодня/будущий день — исключаем пролёты из будущего (rx_start_time > now_utc).
        try:
            day_date = day.date() if isinstance(day, datetime) else day
        except Exception:
            day_date = day
        is_today = hasattr(day_date, "isoformat") and str(day_date) == str(now_utc.date())
        if up_to_datetime is None and hasattr(day_date, "isoformat") and str(day_date) >= str(now_utc.date()):
            up_to_datetime = now_utc

        stats_comm, totals_comm = self.db_manager.get_commercial_passes_stats_by_station(
            day, up_to_datetime, pass_type="коммерческий"
        )
        stats_test, totals_test = self.db_manager.get_commercial_passes_stats_by_station(
            day, up_to_datetime, pass_type="тестовый коммерческий"
        )
        totals = {
            "planned": int(totals_comm.get("planned", 0) or 0) + int(totals_test.get("planned", 0) or 0),
            "successful": int(totals_comm.get("successful", 0) or 0) + int(totals_test.get("successful", 0) or 0),
            "not_received": int(totals_comm.get("not_received", 0) or 0) + int(totals_test.get("not_received", 0) or 0),
        }
        planned_full_day = None
        planned_remaining_today = None
        if is_today:
            planned_full_day = self.db_manager.get_commercial_passes_planned_count(day_date)
            planned_up_to_now = self.db_manager.get_commercial_passes_planned_count(
                day_date, up_to_datetime=up_to_datetime
            )
            planned_remaining_today = max(0, int(planned_full_day) - int(planned_up_to_now))
        if (not stats_comm and not stats_test) and totals["planned"] == 0:
            if is_today and planned_full_day:
                if isinstance(day, datetime):
                    date_display = day.date().isoformat()
                elif hasattr(day, "isoformat"):
                    date_display = day.isoformat()
                else:
                    date_display = str(day)
                return {
                    "date_display": date_display,
                    "rows": [],
                    "totals": {
                        "planned": totals["planned"],
                        "successful": totals["successful"],
                        "not_received": totals["not_received"],
                        "not_received_percent": 0.0,
                    },
                    "not_received_list": [],
                    "planned_remaining_today": planned_remaining_today,
                    "planned_full_day": planned_full_day,
                }
            return None
        if isinstance(day, datetime):
            date_display = day.date().isoformat()
        elif hasattr(day, "isoformat"):
            date_display = day.isoformat()
        else:
            date_display = str(day)
        # Таблица агрегирована по станции (тип показываем только в списке непринятых).
        by_station: Dict[str, Dict[str, int]] = {}
        for stats in (stats_comm or {}, stats_test or {}):
            for station_name, s in stats.items():
                st = str(station_name)
                if st not in by_station:
                    by_station[st] = {"planned": 0, "successful": 0, "not_received": 0}
                by_station[st]["planned"] += int(s.get("planned", 0) or 0)
                by_station[st]["successful"] += int(s.get("successful", 0) or 0)
                by_station[st]["not_received"] += int(s.get("not_received", 0) or 0)
        rows = []
        for station_name, s in by_station.items():
            planned = int(s.get("planned", 0) or 0)
            successful = int(s.get("successful", 0) or 0)
            not_received = int(s.get("not_received", 0) or 0)
            pct = (not_received * 100.0 / planned) if planned else 0.0
            rows.append((station_name, planned, successful, not_received, pct))
        total_pct = (totals["not_received"] * 100.0 / totals["planned"]) if totals["planned"] else 0.0
        not_received_list = self.db_manager.get_commercial_passes_not_received_list(day, up_to_datetime)

        # Сколько ещё запланировано до конца текущих суток (UTC): считаем только для "сегодня".
        # Сколько ещё запланировано до конца текущих суток (UTC): считаем только для "сегодня".

        return {
            "date_display": date_display,
            "rows": rows,
            "totals": {
                "planned": totals["planned"],
                "successful": totals["successful"],
                "not_received": totals["not_received"],
                "not_received_percent": total_pct,
            },
            "not_received_list": not_received_list,
            "planned_remaining_today": planned_remaining_today,
            "planned_full_day": planned_full_day,
        }

    # Вывод статистики коммерческих пролётов за день в консоль.
    def print_comm_day_stats(self, stats) -> None:
        """Печатает статистику коммерческих пролётов за день."""
        if not stats:
            self.logger.info("no commercial stats for day")
            return
        if not stats.get("rows") and stats.get("totals", {}).get("planned", 0) == 0:
            planned_full_day = stats.get("planned_full_day")
            if planned_full_day:
                self.logger.info(f"запланировано {int(planned_full_day)} витков")
                return
        date_display = stats["date_display"]
        print(f"\n{Fore.BLUE + Style.BRIGHT}КОММЕРЧЕСКИЕ ПРОЛЁТЫ ЗА ДЕНЬ  {date_display}")
        print(f"{Fore.BLUE}{'Станция':<30} {'Заказано':>12} {'Принято':>12} {'Не принято':>14} {'% не принято':>15}")
        print(f"{Fore.BLUE}{'-' * 90}")
        rows_sorted = sorted(stats["rows"], key=lambda r: float(r[4] or 0))
        for station_name, planned, successful, not_received, pct in rows_sorted:
            row_color = Fore.BLUE
            if float(pct) > 25:
                row_color = Fore.RED
            elif float(pct) > 5:
                row_color = Fore.YELLOW
            else:
                row_color = Fore.GREEN
            print(
                f"{row_color}{station_name:<30} {int(planned):>12} {int(successful):>12} "
                f"{int(not_received):>14} {float(pct):>14.1f}%"
            )
        totals = stats["totals"]
        print(f"{Fore.BLUE}{'-' * 90}")
        print(
            f"{Fore.GREEN + Style.BRIGHT}{'ИТОГО':<30} {totals['planned']:>12} {totals['successful']:>12} "
            f"{totals['not_received']:>14} {totals['not_received_percent']:>14.1f}%"
        )
        # Для "сегодня" показываем, сколько ещё запланировано до конца суток (UTC)
        remaining = stats.get("planned_remaining_today")
        if remaining is not None:
            print(f"{Fore.BLUE}Планируется до конца дня (UTC): {int(remaining)}")
        not_received_list = stats.get("not_received_list") or []
        if not_received_list:
            print(f"\n{Fore.BLUE + Style.BRIGHT}НЕ ПРИНЯТЫЕ КОММЕРЧЕСКИЕ ПРОЛЁТЫ ЗА ДЕНЬ")
            for item in not_received_list:
                # (station, pass_type, satellite, rx_start, rx_end, graph_url) — pass_type может отсутствовать в старой БД
                if len(item) >= 6:
                    station_name, pass_type, satellite_name, rx_start, rx_end, graph_url = item[:6]
                else:
                    station_name, satellite_name, rx_start, rx_end, graph_url = item[:5]
                    pass_type = "коммерческий"
                type_label = "тестовый" if "тест" in str(pass_type).lower() else "коммерческий"
                time_str = f"{rx_start} — {rx_end}" if rx_end else rx_start
                line = f"  {Fore.BLUE}{station_name} | {type_label} | {satellite_name} | {time_str}"
                if graph_url:
                    line += f" | {graph_url}"
                print(line)

    # Сбор статистики коммерческих пролётов за период.
    def build_comm_period_stats(self, start_day, end_day, up_to_datetime: Optional[datetime] = None) -> dict:
        """Собирает статистику коммерческих пролётов за период."""
        now_utc = datetime.now(timezone.utc)
        # Если период включает сегодня/будущее — исключаем пролёты из будущего (rx_start_time > now_utc).
        if up_to_datetime is None:
            try:
                end_date = end_day.date() if isinstance(end_day, datetime) else end_day
            except Exception:
                end_date = end_day
            if hasattr(end_date, "isoformat") and str(end_date) >= str(now_utc.date()):
                up_to_datetime = now_utc
        stats_comm, totals_comm = self.db_manager.get_commercial_passes_stats_by_station_range(
            start_day, end_day, up_to_datetime=up_to_datetime, pass_type="коммерческий"
        )
        stats_test, totals_test = self.db_manager.get_commercial_passes_stats_by_station_range(
            start_day, end_day, up_to_datetime=up_to_datetime, pass_type="тестовый коммерческий"
        )
        totals = {
            "planned": int(totals_comm.get("planned", 0) or 0) + int(totals_test.get("planned", 0) or 0),
            "successful": int(totals_comm.get("successful", 0) or 0) + int(totals_test.get("successful", 0) or 0),
            "not_received": int(totals_comm.get("not_received", 0) or 0) + int(totals_test.get("not_received", 0) or 0),
        }
        if (not stats_comm and not stats_test) and totals["planned"] == 0:
            return {}
        if isinstance(start_day, datetime):
            start_display = start_day.date().isoformat()
        elif hasattr(start_day, "isoformat"):
            start_display = start_day.isoformat()
        else:
            start_display = str(start_day)
        if isinstance(end_day, datetime):
            end_display = end_day.date().isoformat()
        elif hasattr(end_day, "isoformat"):
            end_display = end_day.isoformat()
        else:
            end_display = str(end_day)
        # Таблица агрегирована по станции (тип показываем только в списке непринятых).
        by_station: Dict[str, Dict[str, int]] = {}
        for stats in (stats_comm or {}, stats_test or {}):
            for station_name, s in stats.items():
                st = str(station_name)
                if st not in by_station:
                    by_station[st] = {"planned": 0, "successful": 0, "not_received": 0}
                by_station[st]["planned"] += int(s.get("planned", 0) or 0)
                by_station[st]["successful"] += int(s.get("successful", 0) or 0)
                by_station[st]["not_received"] += int(s.get("not_received", 0) or 0)
        rows = []
        for station_name, s in by_station.items():
            planned = int(s.get("planned", 0) or 0)
            successful = int(s.get("successful", 0) or 0)
            not_received = int(s.get("not_received", 0) or 0)
            pct = (not_received * 100.0 / planned) if planned else 0.0
            rows.append((station_name, planned, successful, not_received, pct))
        total_pct = (totals["not_received"] * 100.0 / totals["planned"]) if totals["planned"] else 0.0
        not_received_list = self.db_manager.get_commercial_passes_not_received_list_range(
            start_day, end_day, up_to_datetime=up_to_datetime
        )
        return {
            "period_display": f"{start_display} — {end_display}",
            "rows": rows,
            "totals": {
                "planned": totals["planned"],
                "successful": totals["successful"],
                "not_received": totals["not_received"],
                "not_received_percent": total_pct,
            },
            "not_received_list": not_received_list,
        }

    # Вывод статистики коммерческих пролётов за период в консоль.
    def print_comm_period_stats(self, stats, title: str) -> None:
        """Печатает статистику коммерческих пролётов за период."""
        if not stats:
            self.logger.info("no commercial period stats")
            return
        period_display = stats["period_display"]
        print(f"\n{Fore.BLUE + Style.BRIGHT}{title}  {period_display}")
        print(f"{Fore.BLUE}{'Станция':<30} {'Заказано':>12} {'Принято':>12} {'Не принято':>14} {'% не принято':>15}")
        print(f"{Fore.BLUE}{'-' * 90}")
        rows_sorted = sorted(stats["rows"], key=lambda r: float(r[4] or 0))
        for station_name, planned, successful, not_received, pct in rows_sorted:
            row_color = Fore.BLUE
            if float(pct) > 25:
                row_color = Fore.RED
            elif float(pct) > 5:
                row_color = Fore.YELLOW
            else:
                row_color = Fore.GREEN
            print(
                f"{row_color}{station_name:<30} {int(planned):>12} {int(successful):>12} "
                f"{int(not_received):>14} {float(pct):>14.1f}%"
            )
        totals = stats["totals"]
        print(f"{Fore.BLUE}{'-' * 90}")
        print(
            f"{Fore.GREEN + Style.BRIGHT}{'ИТОГО':<30} {totals['planned']:>12} {totals['successful']:>12} "
            f"{totals['not_received']:>14} {totals['not_received_percent']:>14.1f}%"
        )
        not_received_list = stats.get("not_received_list") or []
        if not_received_list:
            print(f"\n{Fore.BLUE + Style.BRIGHT}НЕ ПРИНЯТЫЕ КОММЕРЧЕСКИЕ ПРОЛЁТЫ")
            for item in not_received_list:
                if len(item) >= 6:
                    station_name, pass_type, satellite_name, rx_start, rx_end, graph_url = item[:6]
                else:
                    station_name, satellite_name, rx_start, rx_end, graph_url = item[:5]
                    pass_type = "коммерческий"
                type_label = "тестовый" if "тест" in str(pass_type).lower() else "коммерческий"
                time_str = f"{rx_start} — {rx_end}" if rx_end else rx_start
                line = f"  {Fore.BLUE}{station_name} | {type_label} | {satellite_name} | {time_str}"
                if graph_url:
                    line += f" | {graph_url}"
                print(line)

    # Сбор статистики коммерческих пролётов за 7 дней.
    def build_comm_week_stats(self, end_day, up_to_datetime: Optional[datetime] = None) -> dict:
        """Собирает статистику коммерческих пролётов за 7 дней."""
        if isinstance(end_day, datetime):
            end_day = end_day.date()
        start_day = end_day - timedelta(days=6)
        return self.build_comm_period_stats(start_day, end_day, up_to_datetime=up_to_datetime)

    # Вывод статистики коммерческих пролётов за неделю.
    def print_comm_week_stats(self, stats) -> None:
        self.print_comm_period_stats(stats, "КОММЕРЧЕСКИЕ ПРОЛЁТЫ ЗА НЕДЕЛЮ")

    # Сбор статистики коммерческих пролётов за месяц.
    def build_comm_month_stats(self, end_day, up_to_datetime: Optional[datetime] = None) -> dict:
        """Собирает статистику коммерческих пролётов за месяц."""
        if isinstance(end_day, datetime):
            end_day = end_day.date()
        start_day = end_day.replace(day=1)
        return self.build_comm_period_stats(start_day, end_day, up_to_datetime=up_to_datetime)

    # Вывод статистики коммерческих пролётов за месяц.
    def print_comm_month_stats(self, stats) -> None:
        self.print_comm_period_stats(stats, "КОММЕРЧЕСКИЕ ПРОЛЁТЫ ЗА МЕСЯЦ")

    # Основной цикл: синхронизация Telegram, загрузка, анализ, статистика, email.
    def main(
        self,
        start_day=None,
        end_day=None,
        email: bool = False,
        debug_email: bool = False,
        weekly_email_to_all: bool = False,
        weekly_email_to_debug: bool = False,
        ):
        """Основной цикл: sync Telegram → backfill → загрузка логов → статистика → email.

        Args:
            start_day: Дата начала (если None и end_day None — сегодня).
            end_day: Дата конца (если None — равно start_day).
            email: Включить отправку email-сводки.
            debug_email: Отладочная отправка (только на debug_recipient).
            weekly_email_to_all: Отправить weekly-сводку на все адреса (To+Cc), игнорируя debug.
            weekly_email_to_debug: Отправить weekly-сводку только на debug_recipient.
        """
        if start_day is None and end_day is None:
            start_day = datetime.now(timezone.utc).date()
            end_day = start_day
        else:
            if start_day is None:
                start_day = end_day
            if end_day is None:
                end_day = start_day

        # Синхронизация коммерческих пролётов из Telegram и запись в БД
        tel_client = TelClient(logger=self.logger, config=self.config)
        sync_result = tel_client.run_comm_passes_sync()
        if sync_result is not None:
            total_msgs, total_passes, passes_list = sync_result
            self.logger.info("Telegram: синхронизация пролётов: сообщений=%s, пролётов=%s" % (total_msgs, total_passes))
            if passes_list:
                try:
                    written = self.db_manager.replace_commercial_passes(passes_list)
                    self.logger.info("БД: записано коммерческих пролётов: %s" % written)
                except Exception as exc:
                    self.logger.exception("Ошибка записи коммерческих пролётов в БД: %s" % exc)
        else:
            self.logger.info("Telegram: синхронизация не выполнена (нет настроек или telethon)")

        # Проверяем наличие данных за последние 7 дней и догружаем при необходимости.
        lookback_start = end_day - timedelta(days=6)
        missing_days = [
            day
            for day in (lookback_start + timedelta(days=i) for i in range(7))
            if not self.db_manager.get_daily_success_stats(day)
        ]
        if missing_days:
            missing_start = min(missing_days)
            missing_end = max(missing_days)
            if not (start_day <= missing_start and end_day >= missing_end):
                self.logger.info(
                    f"backfill missing stats for {missing_start}..{missing_end}"
                )
                self._download_and_analyze_range(missing_start, missing_end)
            else:
                self.logger.info(
                    f"missing stats fall within requested range: {missing_start}..{missing_end}"
                )

        self._download_and_analyze_range(start_day, end_day)

        # Выводим статистику по дням за указанный диапазон
        current_day = start_day
        while current_day <= end_day:

            daily_stats = self.buily_daily_pass_stats(current_day)
            self.print_log_day_stats(daily_stats)

            # Коммерческие пролёты за день
            now_utc = datetime.now(timezone.utc)
            is_today = (current_day == now_utc.date())
            comm_up_to = now_utc if is_today else None
            comm_day_stats = self.build_comm_day_stats(current_day, up_to_datetime=comm_up_to)
            self.print_comm_day_stats(comm_day_stats)

            # Статистика за 7 дней (все пролёты, затем коммерческие)
            week_stats = self.build_week_pass_stats(current_day)
            self.print_log_week_stats(week_stats)
            comm_week_up_to = now_utc if current_day >= now_utc.date() else None
            comm_week_stats = self.build_comm_week_stats(current_day, up_to_datetime=comm_week_up_to)
            self.print_comm_week_stats(comm_week_stats)

            # When explicitly sending weekly to all/debug, suppress daily email.
            if daily_stats and (email or debug_email) and not (weekly_email_to_all or weekly_email_to_debug):
                target_date = current_day.strftime("%Y%m%d")
                stations_filter = self.config.get("stations_for_email") or []
                if isinstance(stations_filter, list):
                    stations_set = {s.strip() for s in stations_filter if s}
                else:
                    stations_set = set()
                all_results = {}
                max_passes = daily_stats.get("max_passes") or []
                failed_graphs = daily_stats.get("failed_graphs") or {}
                # только станции, попадающие в письмо
                if stations_set:
                    failed_graphs = {k: v for k, v in failed_graphs.items() if k in stations_set}
                max_by_station = {p.station_name: p for p in max_passes}
                for station_name, total, success, failed, failed_percent, snr_awg in daily_stats["rows"]:
                    if stations_set and station_name not in stations_set:
                        continue
                    total_i = int(total or 0)
                    failed_i = int(failed or 0)
                    best = max_by_station.get(station_name)
                    max_snr_filename = ""
                    if best and getattr(best, "log_path", None):
                        max_snr_filename = os.path.basename(best.log_path) or ""
                    elif best and getattr(best, "pass_id", None):
                        max_snr_filename = str(best.pass_id)
                    unsuccessful_filenames = list(failed_graphs.get(station_name) or [])
                    all_results[station_name] = {
                        "files": total_i,
                        "total_files": total_i,
                        "successful_passes": total_i - failed_i,
                        "unsuccessful_passes": failed_i,
                        "avg_snr": float(snr_awg or 0.0),
                        "max_snr_filename": max_snr_filename,
                        "unsuccessful_filenames": unsuccessful_filenames,
                    }
                # Каталог для графиков: report/YYYY/MM/DD (совпадает со структурой EusLogDownloader)
                base_dir = os.path.dirname(os.path.abspath(__file__))
                report_dir = Path(self.config.get("report_dir") or os.path.join(base_dir, "report"))
                year = current_day.strftime("%Y")
                month = current_day.strftime("%m")
                day = current_day.strftime("%d")
                graphs_dir = report_dir / year / month / day
                graphs_dir.mkdir(parents=True, exist_ok=True)
                summary_7d_path = graphs_dir / "overall_unsuccessful_7d.png"
                generated_7d = self.graph_generator.generate_overall_unsuccessful_7d(
                    target_date, summary_7d_path, days=7,
                    stations_filter=stations_set if stations_set else None,
                )
                summary_7d_chart_path = generated_7d if generated_7d and generated_7d.exists() else None
                # График % пустых за 7 дней по каждой станции
                for station_name in all_results:
                    station_7d_path = graphs_dir / station_name / "unsuccessful_7d.png"
                    generated_station_7d = self.graph_generator.generate_station_unsuccessful_7d(
                        station_name, target_date, station_7d_path, days=7
                    )
                    if generated_station_7d and generated_station_7d.exists():
                        all_results[station_name]["station_7d_chart_path"] = str(generated_station_7d)
                # Скачивание лучших графиков только по станциям из письма
                max_passes_for_email = [p for p in max_passes if p.station_name in all_results]
                if max_passes_for_email:
                    try:
                        self.eus.download_graphs_file(
                            max_passes_for_email,
                            out_dir=str(report_dir),
                            max_parallel=5,
                            retries=2,
                        )
                        for p in max_passes_for_email:
                            if getattr(p, "graph_path", None) and p.station_name in all_results:
                                all_results[p.station_name]["best_graph_path"] = p.graph_path
                            # Сохраняем путь к скачанному графику приёма в БД (all_passes.graph_path)
                            if getattr(p, "graph_path", None):
                                try:
                                    self.db_manager.update_pass_graph_artifacts(p)
                                except Exception as exc:
                                    self.logger.warning(f"DB: update graph_path failed: {exc}")
                    except Exception as e:
                        self.logger.warning(f"download best-pass graphs failed: {e}")
                now_utc = datetime.now(timezone.utc)
                comm_planned = self.db_manager.get_commercial_passes_planned_count(
                    current_day, up_to_datetime=now_utc
                )
                comm_received = self.db_manager.get_commercial_passes_received_count(
                    current_day, up_to_datetime=now_utc
                )
                self.logger.info(
                    "Коммерческих пролётов заказано (на текущий момент UTC): %s, принято: %s"
                    % (comm_planned, comm_received)
                )
                email_cfg = (self.config or {}).get("email") or {}
                debug_recipient = email_cfg.get("debug_recipient") if debug_email else None
                settings = self.email_client.get_email_settings(debug_recipient=debug_recipient)
                if not settings.get("enabled"):
                    self.logger.info("email disabled by settings")
                elif debug_email and not debug_recipient:
                    self.logger.warning("debug email enabled but EMAIL_DEBUG_RECIPIENT is not set")
                else:
                    comm_stats_comm, comm_totals_comm = self.db_manager.get_commercial_passes_stats_by_station(
                        current_day, up_to_datetime=now_utc, pass_type="коммерческий"
                    )
                    comm_stats_test, comm_totals_test = self.db_manager.get_commercial_passes_stats_by_station(
                        current_day, up_to_datetime=now_utc, pass_type="тестовый коммерческий"
                    )
                    comm_rows_typed = []
                    for station_name, s in (comm_stats_comm or {}).items():
                        comm_rows_typed.append(
                            (
                                str(station_name),
                                "коммерческий",
                                int(s.get("planned", 0) or 0),
                                int(s.get("successful", 0) or 0),
                                int(s.get("not_received", 0) or 0),
                            )
                        )
                    for station_name, s in (comm_stats_test or {}).items():
                        comm_rows_typed.append(
                            (
                                str(station_name),
                                "тестовый",
                                int(s.get("planned", 0) or 0),
                                int(s.get("successful", 0) or 0),
                                int(s.get("not_received", 0) or 0),
                            )
                        )
                    comm_totals = {
                        "planned": int(comm_totals_comm.get("planned", 0) or 0) + int(comm_totals_test.get("planned", 0) or 0),
                        "successful": int(comm_totals_comm.get("successful", 0) or 0) + int(comm_totals_test.get("successful", 0) or 0),
                        "not_received": int(comm_totals_comm.get("not_received", 0) or 0) + int(comm_totals_test.get("not_received", 0) or 0),
                    }
                    comm_7d_path = graphs_dir / "comm_unsuccessful_7d.png"
                    generated_comm_7d = self.graph_generator.generate_comm_unsuccessful_7d(
                        target_date, comm_7d_path, days=7, up_to_datetime=now_utc
                    )
                    comm_summary_7d_chart_path = (
                        generated_comm_7d if generated_comm_7d and generated_comm_7d.exists() else None
                    )
                    comm_not_received_list = self.db_manager.get_commercial_passes_not_received_list(
                        current_day, up_to_datetime=now_utc
                    )
                    body, inline_images = self.email_client.build_stats_email_body(
                        target_date,
                        all_results,
                        graphs_dir=graphs_dir,
                        summary_7d_chart_path=summary_7d_chart_path,
                        comm_totals=comm_totals,
                        comm_rows_typed=comm_rows_typed,
                        comm_summary_7d_chart_path=comm_summary_7d_chart_path,
                        comm_not_received_list=comm_not_received_list,
                    )
                    sent = self.email_client.send_stats_email(
                        smtp_server=settings["smtp_server"],
                        smtp_port=settings["smtp_port"],
                        sender_email=settings["sender_email"],
                        sender_password=settings["sender_password"],
                        recipients=settings["recipients"],
                        cc_recipients=settings.get("cc_recipients"),
                        subject=settings["subject"],
                        body=body,
                        attachments=None,
                        inline_images=inline_images,
                    )
                    if sent:
                        self.logger.info(f"email sent for {target_date}")
                    else:
                        self.logger.warning(f"email failed for {target_date}")

            # Дополнительное еженедельное письмо со статистикой за последние 7 дней:
            # отправляется по воскресеньям (UTC) один раз за запуск, даже если за день нет данных.
            # Управление получателями weekly-письма отдельными флагами (all/debug).
            if email or debug_email or weekly_email_to_all or weekly_email_to_debug:
                # Weekly should be based on report day (current_day), not "today"
                if current_day.weekday() == 6 and current_day == end_day:
                    week_end = current_day
                    week_start = week_end - timedelta(days=6)
                    target_date = current_day.strftime("%Y%m%d")

                    stations_filter = self.config.get("stations_for_email") or []
                    if isinstance(stations_filter, list):
                        stations_set = {s.strip() for s in stations_filter if s}
                    else:
                        stations_set = set()

                    base_dir = os.path.dirname(os.path.abspath(__file__))
                    report_dir = Path(self.config.get("report_dir") or os.path.join(base_dir, "report"))
                    year = current_day.strftime("%Y")
                    month = current_day.strftime("%m")
                    day = current_day.strftime("%d")
                    graphs_dir = report_dir / year / month / day
                    graphs_dir.mkdir(parents=True, exist_ok=True)

                    summary_7d_path = graphs_dir / "overall_unsuccessful_7d.png"
                    generated_7d = self.graph_generator.generate_overall_unsuccessful_7d(
                        target_date,
                        summary_7d_path,
                        days=7,
                        stations_filter=stations_set if stations_set else None,
                    )
                    summary_7d_chart_path = (
                        generated_7d if generated_7d and generated_7d.exists() else None
                    )

                    now_utc = datetime.now(timezone.utc)
                    comm_7d_path = graphs_dir / "comm_unsuccessful_7d.png"
                    generated_comm_7d = self.graph_generator.generate_comm_unsuccessful_7d(
                        target_date, comm_7d_path, days=7, up_to_datetime=now_utc
                    )
                    comm_summary_7d_chart_path = (
                        generated_comm_7d if generated_comm_7d and generated_comm_7d.exists() else None
                    )

                    email_cfg = (self.config or {}).get("email") or {}
                    if weekly_email_to_debug:
                        debug_recipient = email_cfg.get("debug_recipient")
                    elif weekly_email_to_all:
                        debug_recipient = None
                    else:
                        debug_recipient = email_cfg.get("debug_recipient") if debug_email else None
                    settings = self.email_client.get_email_settings(debug_recipient=debug_recipient)
                    if not settings.get("enabled"):
                        self.logger.info("weekly email: disabled by settings")
                    elif (weekly_email_to_debug or debug_email) and not debug_recipient:
                        self.logger.warning("weekly email: debug enabled but EMAIL_DEBUG_RECIPIENT is not set")
                    else:
                        weekly_rows = self.db_manager.get_range_station_stats(week_start, week_end)
                        weekly_results: Dict[str, Dict[str, Any]] = {}
                        for station_name, total, success, failed, _, snr_awg in weekly_rows:
                            if stations_set and station_name not in stations_set:
                                continue
                            weekly_results[str(station_name)] = {
                                "files": int(total or 0),
                                "total_files": int(total or 0),
                                "successful_passes": int(success or 0),
                                "unsuccessful_passes": int(failed or 0),
                                "avg_snr": float(snr_awg or 0.0),
                                "max_snr_filename": "",
                                "unsuccessful_filenames": [],
                            }

                        # График % пустых за 7 дней по каждой станции для weekly-письма
                        for station_name in weekly_results:
                            station_7d_path = graphs_dir / station_name / "unsuccessful_7d.png"
                            generated_station_7d = self.graph_generator.generate_station_unsuccessful_7d(
                                station_name, target_date, station_7d_path, days=7
                            )
                            if generated_station_7d and generated_station_7d.exists():
                                weekly_results[station_name]["station_7d_chart_path"] = str(generated_station_7d)

                        comm_stats_week_comm, comm_totals_week_comm = (
                            self.db_manager.get_commercial_passes_stats_by_station_range(
                                week_start, week_end, up_to_datetime=now_utc, pass_type="коммерческий"
                            )
                        )
                        comm_stats_week_test, comm_totals_week_test = (
                            self.db_manager.get_commercial_passes_stats_by_station_range(
                                week_start, week_end, up_to_datetime=now_utc, pass_type="тестовый коммерческий"
                            )
                        )
                        comm_rows_week_typed = []
                        for station_name, s in (comm_stats_week_comm or {}).items():
                            comm_rows_week_typed.append(
                                (
                                    str(station_name),
                                    "коммерческий",
                                    int(s.get("planned", 0) or 0),
                                    int(s.get("successful", 0) or 0),
                                    int(s.get("not_received", 0) or 0),
                                )
                            )
                        for station_name, s in (comm_stats_week_test or {}).items():
                            comm_rows_week_typed.append(
                                (
                                    str(station_name),
                                    "тестовый",
                                    int(s.get("planned", 0) or 0),
                                    int(s.get("successful", 0) or 0),
                                    int(s.get("not_received", 0) or 0),
                                )
                            )
                        comm_totals_week = {
                            "planned": int(comm_totals_week_comm.get("planned", 0) or 0) + int(comm_totals_week_test.get("planned", 0) or 0),
                            "successful": int(comm_totals_week_comm.get("successful", 0) or 0) + int(comm_totals_week_test.get("successful", 0) or 0),
                            "not_received": int(comm_totals_week_comm.get("not_received", 0) or 0) + int(comm_totals_week_test.get("not_received", 0) or 0),
                        }
                        comm_not_received_week = (
                            self.db_manager.get_commercial_passes_not_received_list_range(
                                week_start, week_end, up_to_datetime=now_utc
                            )
                        )

                        sent_week = self.email_client.send_weekly_stats_email(
                            settings=settings,
                            target_date=target_date,
                            week_start=week_start,
                            week_end=week_end,
                            weekly_results=weekly_results,
                            graphs_dir=graphs_dir,
                            summary_7d_chart_path=summary_7d_chart_path,
                            comm_totals=comm_totals_week,
                            comm_rows_typed=comm_rows_week_typed,
                            comm_summary_7d_chart_path=comm_summary_7d_chart_path,
                            comm_not_received_list=comm_not_received_week,
                        )
                        if sent_week:
                            self.logger.info(f"weekly email sent for {week_start}..{week_end}")
            current_day += timedelta(days=1)

    # Загрузка логов EUS, анализ пролётов и запись в БД.
    def _download_and_analyze_range(self, start_day, end_day) -> None:
        """Загружает логи с EUS, анализирует пролёты и записывает в БД.

        Args:
            start_day: Дата начала диапазона.
            end_day: Дата конца диапазона.
        """
        base_dir = os.path.dirname(__file__)
        passes_logs_dir = os.path.join(base_dir, "passes_logs")
        # Загружаем HTML страницу со всем диапазоном дат.
        # дата начала
        start_dt = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
        # дата конца
        end_dt = datetime.combine(end_day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

        # параметры даты
        params = (start_dt, end_dt)

        try:
            # загружаем HTML страницу со всем диапазоном дат
            page_passes_all = self.eus.load_html_and_parse(params=params)
        except TimeoutError as exc:
            self.logger.warning(f"load_html_and_parse timeout: {exc}")
            return
        except Exception as exc:
            self.logger.exception("load_html_and_parse failed", exc_info=exc)
            return

        all_passes = []

        for passes in page_passes_all.values():
            all_passes.extend(passes)

        if not all_passes:
            self.logger.info("no passes found for date range")
            return

        downloaded = []
        for station_name, passes in page_passes_all.items():
            if not passes:
                continue
            try:
                t0 = time.perf_counter()
                station_results = self.eus.download_logs_file(passes, out_dir=passes_logs_dir)
                elapsed = time.perf_counter() - t0
                self.logger.info(f"logs download time: station={station_name} time={elapsed:.2f}s")
                downloaded.extend(station_results)
            except Exception as exc:
                self.logger.exception(f"download_logs_file failed for station {station_name}", exc_info=exc)
                continue

        # анализируем пролета
        analyzed = [p for p in (self.analyzer.analyze_pass(item) for item in downloaded) if p is not None]

        # список для хранения пролетов
        ready = []
        for sat_pass in analyzed:
            # если лог-путь не найден, то пропускаем
            if not sat_pass.log_path:
                continue
            # если дата или время начала не найдены, то пропускаем
            if sat_pass.pass_date is None or sat_pass.pass_start_time is None:
                continue
            # добавляем пролета в список
            ready.append(sat_pass)

        if ready:
            try:
                inserted = self.db_manager.add_passes_batch(ready)
                self.logger.info(f"batch insert complete: inserted={inserted}")
            except Exception as exc:
                self.logger.exception("batch insert failed", exc_info=exc)


if __name__ == "__main__":

    # путь к каталогу для сохранения логов
    BASE_DIR = Path("/root/lorett/GroundLinkServer")
    PATH_LOG = str(BASE_DIR / "server_logs") + os.sep

    # парсинг аргументов командной строки
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", nargs="?", help="Дата начала (YYYYMMDD)")
    parser.add_argument("end_date", nargs="?", help="Дата завершения (YYYYMMDD)")
    parser.add_argument("--sch", action="store_true", help="Запуск в 00:00 UTC по расписанию")
    parser.add_argument("--off-email", action="store_true", help="Отключить отправку email")
    parser.add_argument("--debag-email", action="store_true", help="Отладочная отправка email")
    weekly_group = parser.add_mutually_exclusive_group()
    weekly_group.add_argument(
        "--weekly-email-all",
        action="store_true",
        help="Weekly-сводка в воскресенье: отправить на все адреса (To+Cc)",
    )
    weekly_group.add_argument(
        "--weekly-email-debug",
        action="store_true",
        help="Weekly-сводка в воскресенье: отправить только на debug_recipient",
    )
    args = parser.parse_args()

    # функция для парсинга даты в формате YYYYMMDD
    def parse_yyyymmdd(value: str):
        return datetime.strptime(value, "%Y%m%d").date()

    # инициализация сервера
    server = GroundLinkServer(path_log=PATH_LOG)

    # парсинг даты начала и конца
    start_day = parse_yyyymmdd(args.start_date) if args.start_date else None
    end_day = parse_yyyymmdd(args.end_date) if args.end_date else None

    # если запуск в 00:00 UTC по расписанию
    if args.sch:

        # пока не наступит 00:00 UTC
        while True:

            # получаем текущую дату и время
            now = datetime.now(timezone.utc)

            # получаем дату и время следующей полночи
            next_midnight = datetime.combine(
                now.date() + timedelta(days=1),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )

            # Ждём до следующей полночи UTC, логируя остаток каждый час (в часах и секундах).
            # Если на следующем запуске будет отправляться weekly-письмо (отчётный день = воскресенье),
            # логируем это также раз в час до отправки.
            next_run_day = next_midnight.date() - timedelta(days=1)
            will_send_weekly = next_run_day.weekday() == 6  # Sunday (UTC)
            while True:
                now = datetime.now(timezone.utc)
                remaining = (next_midnight - now).total_seconds()
                if remaining <= 0:
                    break
                hours = int(remaining // 3600)
                seconds = int(remaining % 3600)
                server.logger.info(f"time until UTC midnight: {hours}h {seconds}s")
                # Weekly email is sent at 00:00 UTC Monday (report day is Sunday).
                if will_send_weekly:
                    server.logger.info(f"time until weekly email send: {hours}h {seconds}s")
                else:
                    # Countdown to the next weekly send (next Monday 00:00 UTC).
                    days_until_monday = (7 - now.weekday()) % 7
                    if days_until_monday == 0:
                        days_until_monday = 7
                    next_weekly_midnight = datetime.combine(
                        now.date() + timedelta(days=days_until_monday),
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    )
                    remaining_weekly = max(0, int((next_weekly_midnight - now).total_seconds()))
                    w_days = remaining_weekly // 86400
                    w_hours = (remaining_weekly % 86400) // 3600
                    w_seconds = remaining_weekly % 3600
                    server.logger.info(
                        f"time until weekly email send: {w_days}d {w_hours}h {w_seconds}s"
                    )
                time.sleep(min(3600, remaining))

            # небольшой буфер, чтобы точно перейти за границу дня
            time.sleep(5)

            # получаем предыдущую дату (отчет за прошедшие сутки)
            run_day = datetime.now(timezone.utc).date() - timedelta(days=1)

            # запускаем сервер
            server.main(
                start_day=run_day,
                end_day=run_day,
                email=not args.off_email,
                debug_email=args.debag_email,
                weekly_email_to_all=args.weekly_email_all,
                weekly_email_to_debug=args.weekly_email_debug,
            )

    else:
        # запускаем сервер
        server.main(
            start_day=start_day,
            end_day=end_day,
            email=not args.off_email, 
            debug_email=args.debag_email,
            weekly_email_to_all=args.weekly_email_all,
            weekly_email_to_debug=args.weekly_email_debug,
        )
