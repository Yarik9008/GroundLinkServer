import argparse
import os
import re
import time
from datetime import datetime, timedelta, timezone
from Logger import Logger
from EusLogDownloader import EusLogDownloader
from DbManager import DbManager
from PassAnalyzer import PassAnalyzer
from SatPass import SatPas
from EmailClient import EmailClient
from colorama import Fore, Style, init as colorama_init
colorama_init(autoreset=True)


class GroundLinkServer:
    
    def __init__(self, path_log:str) -> None:
        # Инициализация основного логера 
        self.logger = Logger(path_log=path_log, log_level="info", logger_name="MAIN")
        
        # инициализация обработчика базы данных 
        self.logger_db = Logger(path_log=path_log, log_level="info", logger_name="DB")
        self.db_manager = DbManager(logger=self.logger_db)

        # uнициализация загрузчика лог файлов 
        self.logger_eus = Logger(path_log=path_log, log_level="info", logger_name="EUS")
        self.eus = EusLogDownloader(logger=self.logger_eus)

        # инициализация анализатора логов
        self.logger_analyzer = Logger(path_log=path_log, log_level="info", logger_name="ANALYZER")
        self.analyzer = PassAnalyzer(logger=self.logger_analyzer)

        # инициализация клиента email
        self.logger_email = Logger(path_log=path_log, log_level="info", logger_name="EMAIL")
        self.email_client = EmailClient(logger=self.logger_email)


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

        # Выводим статистику по дням за указанный диапазон
        current_day = start_day
        while current_day <= end_day:
            daily_stats = self.buily_daily_pass_stats(current_day)
            self.print_log_day_stats(daily_stats)
            
            if daily_stats and (email or debug_email):
                target_date = current_day.strftime("%Y%m%d")
                all_results = {}
                for station_name, total, success, failed, failed_percent, snr_awg in daily_stats["rows"]:
                    all_results[station_name] = {
                        "total_files": int(total or 0),
                        "unsuccessful_passes": int(failed or 0),
                        "avg_snr": float(snr_awg or 0.0),
                    }
                debug_recipient = os.getenv("EMAIL_DEBUG_RECIPIENT") if debug_email else None
                settings = self.email_client.get_email_settings({}, debug_recipient=debug_recipient)
                if not settings.get("enabled"):
                    self.logger.info("email disabled by settings")
                elif debug_email and not debug_recipient:
                    self.logger.warning("debug email enabled but EMAIL_DEBUG_RECIPIENT is not set")
                else:
                    body, inline_images = self.email_client.build_stats_email_body(
                        target_date,
                        all_results,
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


if __name__ == "__main__":

    # путь к каталогу для сохранения логов
    PATH_LOG = "C:/Users/Yarik/YandexDisk/Engineering_local/Soft/GroundLinkMonitorServer/server_logs/"

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
