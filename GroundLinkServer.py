import argparse
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

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
    
    def __init__(self, path_log:str) -> None:
        # Инициализация основного логера
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


    def _load_config(self, path_log: str) -> dict:
        """Загружает config.json из каталога скрипта. При ошибке возвращает {}."""
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


    def buily_daily_pass_stats(self, day):
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

    def print_log_day_stats(self, stats) -> None:
        """Печатает статистику успешных пролетов за день."""
        if not stats:
            self.logger.info("no daily stats found")
            return
        date_display = stats["date_display"]

        print(f"\n{Fore.CYAN + Style.BRIGHT}ИТОГОВАЯ СВОДКА ПО ВСЕМ СТАНЦИЯМ  {date_display}")
        print(f"{Fore.CYAN}{'Станция':<30} {'Всего':>10} {'Успешных':>12} {'Пустых':>14} {'% пустых':>15} {'Средний SNR':>15}")
        print(f"{Fore.CYAN}{'-' * 110}")

        for station_name, total, success, failed, failed_percent, snr_awg in stats["rows"]:
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

        max_passes = stats["max_passes"]

        if max_passes:
            print(f"\n{Fore.CYAN + Style.BRIGHT}ФАЙЛЫ С МАКСИМАЛЬНОЙ СУММОЙ SNR ПО СТАНЦИЯМ")
            print(f"{Fore.CYAN}{'Станция':<30} {'Файл/ID':<80} {'Сумма SNR':>15}")
            print(f"{Fore.CYAN}{'-' * 130}")
            for sat_pass in max_passes:
                file_or_id = sat_pass.log_path or sat_pass.pass_id or ""
                print(f"{Fore.CYAN}{sat_pass.station_name:<30} {file_or_id:<80} {float(sat_pass.snr_sum or 0):>15.2f}")


    def main(
        self, 
        start_day=None, # дата начала
        end_day=None, # дата конца 
        email: bool = False, # отправка email
        debug_email: bool = False # отладочная отправка email
        ):
        # Определяем диапазон дат.
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

            if daily_stats and (email or debug_email):
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
                debug_recipient = (
                    email_cfg.get("debug_recipient")
                    or os.getenv("EMAIL_DEBUG_RECIPIENT")
                ) if debug_email else None
                settings = self.email_client.get_email_settings(debug_recipient=debug_recipient)
                if not settings.get("enabled"):
                    self.logger.info("email disabled by settings")
                elif debug_email and not debug_recipient:
                    self.logger.warning("debug email enabled but EMAIL_DEBUG_RECIPIENT is not set")
                else:
                    comm_stats, comm_totals = self.db_manager.get_commercial_passes_stats_by_station(
                        current_day, up_to_datetime=now_utc
                    )
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
                        comm_stats=comm_stats,
                        comm_totals=comm_totals,
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
            current_day += timedelta(days=1)


    def _download_and_analyze_range(self, start_day, end_day) -> None:
        # путь к каталогу для сохранения логов
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

            # вычисляем количество секунд до следующей полночи и если больше 0, то спим до следующей полночи
            sleep_seconds = (next_midnight - now).total_seconds()
            if sleep_seconds > 0:
                server.logger.info(f"sleep until UTC midnight: {sleep_seconds:.0f}s")
                time.sleep(sleep_seconds)

            # получаем текущую дату
            run_day = datetime.now(timezone.utc).date()

            # запускаем сервер
            server.main(
                start_day=run_day,
                end_day=run_day + timedelta(days=1),
                email=args.off_email,
                debug_email=args.debag_email,
            )

    else:
        # запускаем сервер
        server.main(
            start_day=start_day,
            end_day=end_day,
            email=not args.off_email, 
            debug_email=args.debag_email,
        )
