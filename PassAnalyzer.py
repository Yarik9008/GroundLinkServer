from __future__ import annotations
import os
import re
from pathlib import Path
from datetime import datetime
from sqlite3 import paramstyle
from typing import Iterable, Optional
from Logger import Logger
from SatPass import SatPas


class PassAnalyzer:
    """Анализатор лог-файлов пролетов.

    Назначение:
        - Разбирает заголовок лога (id, спутник, станция, координаты, даты).
        - Парсит таблицу измерений и считает SNR-метрики.
        - Определяет окно приема по State (если есть) или по порогу SNR.
        - Заполняет и возвращает объекты SatPas.

    Методы:
        - __init__: инициализация анализатора.
        - extract_pass_params: парсит заголовок лога и возвращает параметры пролета.
        - parse_lines: парсит строки измерений и возвращает таблицу значений.
        - extract_snr_metrics: извлекает SNR-метрики и окно приема.
        - analyze_passes: анализирует список пролетов и заполняет SatPas.
    """

    # Инициализация анализатора
    def __init__(self, logger: Logger) -> None:
        if logger is None:
            raise ValueError("logger is required")

        self.logger = logger

        # пороговый уровень SNR по умолчанию 
        self.snr_trigger_level = 6

    # возврвщает параметры лог файла 
    def extract_pass_params(self, log_lines: list) -> dict:
        """Парсит заголовок лога и возвращает ключевые параметры."""
        pass_id = None
        satellite = None
        start_time = None
        pass_date = None
        stop_time = None
        station = None
        location = None

        data_lines = []
        for line in log_lines:

            line = line.strip()
            # сохраняем строки с данными
            if line and not line.startswith("#"):
                data_lines.append(line)
            # пропускаем строки, не начинающиеся с #
            if not line.startswith("#"):
                continue

            # извлекаем параметры пролета
            # извлекаем ID пролета
            if line.startswith("#Pass ID:"):
                pass_id = line.split(":", 1)[1].strip()

            # извлекаем название спутника
            elif line.startswith("#Satellite:"):
                satellite = line.split(":", 1)[1].strip()

            # извлекаем время старта
            elif line.startswith("#Start time:"):
                raw_value = line.split(":", 1)[1].strip()
                start_time = self._parse_datetime(raw_value)
                if isinstance(start_time, datetime):
                    pass_date = start_time.date().isoformat()
                else:
                    start_time = raw_value
                    try:
                        pass_date = datetime.fromisoformat(raw_value[:10]).date().isoformat()
                    except ValueError:
                        pass_date = None

            # извлекаем название станции
            elif line.startswith("#Station:"):

                station = line.split(":", 1)[1].strip()

            # извлекаем координаты станции
            elif line.startswith("#Location:"):
                raw_location = line.split(":", 1)[1].strip()
                tokens = raw_location.split()
                if len(tokens) >= 2:
                    try:
                        if "lon" in tokens and "lat" in tokens:
                            lon_idx = tokens.index("lon")
                            lat_idx = tokens.index("lat")
                            lon = float(tokens[lon_idx - 1]) if lon_idx > 0 else None
                            lat = float(tokens[lat_idx - 1]) if lat_idx > 0 else None
                        else:
                            lon = float(tokens[0])
                            lat = float(tokens[1])

                        if lon is None or lat is None:
                            raise ValueError("longitude/latitude not found")

                        location = (lat, lon)

                    except (ValueError, IndexError) as exc:
                        self.logger.debug(f"Error: {exc}")
                        location = None

                else:
                    self.logger.debug(f"Error: {len(tokens)}")
                    location = None


            # извлекаем время окончания приема
            elif line.startswith("#Closed at:"):
                raw_value = line.split(":", 1)[1].strip()
                stop_time = self._parse_datetime(raw_value)
                if stop_time is None:
                    stop_time = raw_value

        # Проверяем последнюю строку данных и при необходимости обновляем stop_time.
        if len(data_lines) >= 1:
            last_line = data_lines[-1].split()
            if len(last_line) >= 2:
                raw_time = f"{last_line[0]} {last_line[1]}"
                last_time = self._parse_datetime(raw_time)

                if last_time is not None:
                    if isinstance(stop_time, datetime):
                        if last_time > stop_time:
                            stop_time = last_time
                    elif stop_time is None:
                        stop_time = last_time


        # возвращаем параметры пролета
        return {
            "pass_id": pass_id,
            "satellite": satellite,
            "start_time": start_time,
            "pass_date": pass_date,
            "station": station,
            "location": location,
            "stop_time": stop_time,
        }

    def _parse_datetime(self, value: str) -> Optional[datetime]:
        """Парсит дату/время с дробными секундами переменной длины."""
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass

        sep = "T" if "T" in value else " "
        if sep not in value:
            return None
        date_part, time_part = value.split(sep, 1)

        if "." in time_part:
            base, frac = time_part.split(".", 1)
            frac = (frac + "000000")[:6]
            normalized = f"{date_part} {base}.{frac}"
            try:
                return datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                return None
        try:
            return datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    # парсит строки записей лога и возвращает список с числовыми значениями
    def parse_lines(self, log_lines: list) -> Optional[list]:
        """Парсит строки записей лога и возвращает список списков.

        Возвращает None, если найдена некорректная запись.
        """
        headers = None
        rows = []
        in_records = False
        base_date = None

        def has_more_data_lines(start_idx: int) -> bool:
            for rest in log_lines[start_idx + 1:]:
                rest = rest.strip()
                if not rest:
                    continue
                if rest.startswith("#Closed at:"):
                    return False
                if rest.startswith("#"):
                    continue
                return True
            return False

        for idx, line in enumerate(log_lines):
            # Убираем переводы строк и пробелы по краям.
            line = line.strip()
            if not line:
                continue

            if line.startswith("#Closed at:"):
                # Маркер окончания записей — выходим из цикла.
                break

            if line.startswith("#"):
                if line.startswith("#Start time:"):
                    raw_value = line.split(":", 1)[1].strip()
                    start_dt = self._parse_datetime(raw_value)
                    base_date = start_dt.date().isoformat() if isinstance(start_dt, datetime) else None
                if line.startswith("#Time"):
                    # Считываем заголовок таблицы и включаем режим чтения записей.
                    header_line = line[1:].strip()
                    headers = header_line.split()
                    in_records = True
                continue

            if not in_records or not headers:
                # Пока не дошли до заголовка — записи не парсим.
                continue

            parts = line.split()
            if not parts:
                continue

            if headers[0] == "Time" and len(parts) == len(headers) + 1:
                # Дата и время разделены на два токена — склеиваем их.
                time_value = f"{parts[0]} {parts[1]}"
                values = [time_value] + parts[2:]
            else:
                values = parts

            if headers[0] == "Time" and values:
                # Если в логе только время, добавляем дату из заголовка.
                time_value = values[0]
                if base_date and " " not in time_value and ":" in time_value:
                    values[0] = f"{base_date} {time_value}"

            if len(values) != len(headers):
                # Некорректная запись — пропускаем весь лог, кроме случая последней строки.
                if not has_more_data_lines(idx):
                    self.logger.warning(f"invalid last log line format, ignore: {line}")
                    break
                self.logger.warning(f"invalid log line format: {line}")
                return None

            numeric_values = []
            for idx, raw_value in enumerate(values):
                if headers[idx] == "Time":
                    # Время парсим в datetime.
                    dt_value = self._parse_datetime(raw_value)
                    if dt_value is None:
                        numeric_values = []
                        break
                    numeric_values.append(dt_value)
                else:
                    try:
                        # Остальные поля пытаемся привести к float.
                        numeric_values.append(float(raw_value))
                    except ValueError:
                        numeric_values = []
                        break

            if not numeric_values:
                # Если парсинг хотя бы одного поля не удался — пропускаем весь лог, кроме последней строки.
                if not has_more_data_lines(idx):
                    self.logger.warning(f"invalid last log line format, ignore: {line}")
                    break
                self.logger.warning(f"invalid log line format: {line}")
                return None

            # Добавляем строку с распарсенными значениями.
            rows.append(numeric_values)

        if headers is None:
            # Заголовок не найден — возвращаем пустой результат.
            return []

        # Первая строка результата — заголовок, далее данные.
        return [headers] + rows

    # извлекает SNR-метрики из списка строк
    def extract_snr_metrics(self, rows: list) -> dict:
        """Извлекает SNR-метрики из списка списков."""
        if not rows:
            # Нет данных — возвращаем пустые метрики.
            self.logger.debug("Нет данных")
            return {
                "snr_sum": None,
                "snr_awg": None,
                "snr_max": None,
                "rx_start_time": None,
                "rx_end_time": None,
                "success": False,
            }

        # Первый элемент — заголовок, далее строки данных.
        headers = rows[0]
        data_rows = rows[1:]
        if not data_rows:
            # Заголовок есть, данных нет.
            self.logger.debug("Заголовок есть, данных нет")
            return {
                "snr_sum": None,
                "snr_awg": None,
                "snr_max": None,
                "rx_start_time": None,
                "rx_end_time": None,
                "success": False,
            }

        # Определяем индекс столбца SNR.
        try:
            # индекс н
            snr_idx = headers.index("SNR")
            self.logger.debug(f"SNR column found at index: {snr_idx}")

        except ValueError:
            self.logger.warning("SNR column not found in log headers")
            return {
                "snr_sum": None,
                "snr_awg": None,
                "snr_max": None,
                "rx_start_time": None,
                "rx_end_time": None,
                "success": False,
            }

        # Определяем индекс столбца Time.
        time_idx = 0 if headers and headers[0] == "Time" else None
        if time_idx is None:
            try:
                time_idx = headers.index("Time")
            except ValueError:
                self.logger.warning("Time column not found in log headers")
                return {
                    "snr_sum": None,
                    "snr_awg": None,
                    "snr_max": None,
                    "rx_start_time": None,
                    "rx_end_time": None,
                    "success": False,
                }

        # Определяем индекс столбца State, если он есть.
        try:
            state_idx = headers.index("State")
        except ValueError:
            state_idx = None

        # Временные границы приема и массивы SNR.
        rx_start_time = None
        rx_end_time = None
        snr_window = []  # SNR внутри окна приема
        snr_all = []     # SNR по всем строкам
        state_used = False
        seen_nonzero_state = False

        for row in data_rows:
            # Защита от строк неправильной длины.
            if len(row) <= max(time_idx, snr_idx):
                continue
            time_value = row[time_idx]
            snr_value = row[snr_idx]
            snr_all.append(snr_value)

            if state_idx is not None and len(row) > state_idx:
                # Если есть State, определяем окно приема по нему.
                state_value = row[state_idx]
                if state_value != 0:
                    seen_nonzero_state = True

                if seen_nonzero_state and state_value == 0:
                    if rx_start_time is None:
                        rx_start_time = time_value
                    rx_end_time = time_value
                    snr_window.append(snr_value)
                    state_used = True
            else:
                # Окно приема по SNR-порогу.
                if snr_value >= self.snr_trigger_level:
                    if rx_start_time is None:
                        rx_start_time = time_value
                    rx_end_time = time_value
                    snr_window.append(snr_value)

        if snr_window:
            # Есть окно приема — считаем метрики по нему.
            sum_snr = sum(snr_window)
            max_snr = max(snr_window)
            awg_snr = round(sum_snr / len(snr_window), 2)
            success = True
        elif snr_all:
            # Окно приема не найдено — считаем по всем строкам.
            sum_snr = sum(snr_all)
            max_snr = max(snr_all)
            awg_snr = round(sum_snr / len(snr_all), 2)
            success = False
        else:
            # Нет валидных чисел SNR.
            sum_snr = None
            max_snr = None
            awg_snr = None
            success = False

        # Возвращаем метрики и временные границы приема.
        return {
            "snr_sum": sum_snr,
            "snr_awg": awg_snr,
            "snr_max": max_snr,
            "rx_start_time": rx_start_time,
            "rx_end_time": rx_end_time,
            "success": success,
        }

    # Анализ пролета и заполнение полей SatPas
    def analyze_pass(self, sat_pass: SatPas) -> Optional[SatPas]:
        """Анализирует один лог пролета и заполняет поля SatPas.

        Args:
            sat_pass: Объект SatPas.

        Returns:
            Optional[SatPas]: Заполненный SatPas или None, если лог пропущен.
        """
        # Проверка наличия лог-файла
        if not sat_pass.log_path:
            self.logger.warning("log file not found: log_path is empty")
            return None
        else:
            self.logger.debug("Путь к лог файлу найден:")
            self.logger.debug(sat_pass.log_path)

        lines = []

        # Открытие указанного лог-файла для дальнейшего чтения.
        try:
            with open(sat_pass.log_path, "r", encoding="utf-8") as log_file:
                self.logger.debug("Лог-файл успешно открыт для чтения")
                self.logger.debug(log_file.name)
                lines = log_file.readlines()
                log_file.close()

        except FileNotFoundError as exc:
            raise FileNotFoundError(f"log file not found: {sat_pass.log_path}") from exc

        except OSError as exc:
            raise OSError(f"failed to read log file: {sat_pass.log_path}") from exc

        # заполняем параметры пролета 
        params = {}

        params.update(self.extract_pass_params(lines))
        parsed_rows = self.parse_lines(lines)
        if parsed_rows is None:
            self.logger.warning(f"skip log with invalid record: {sat_pass.log_path}")
            return None
        params.update(self.extract_snr_metrics(parsed_rows))

        # Fallback: вытаскиваем station/satellite из имени файла, если нет в заголовке.
        if (not params.get("station") or not params.get("satellite")) and sat_pass.log_path:
            base_name = os.path.basename(sat_pass.log_path)
            station_from_name = None
            satellite_from_name = None
            if "__" in base_name:
                station_from_name, rest = base_name.split("__", 1)
            else:
                rest = base_name
            rest = rest.replace("_rec.log", "").replace(".log", "")
            parts = rest.split("_")
            if len(parts) >= 3:
                satellite_from_name = "_".join(parts[2:])
            if not params.get("station") and station_from_name:
                params["station"] = station_from_name
            if not params.get("satellite") and satellite_from_name:
                params["satellite"] = satellite_from_name.replace("_", " ")

        # Fallback: вытаскиваем дату/время из имени файла, если нет в заголовке.
        if (not params.get("pass_date") or not params.get("start_time")) and sat_pass.log_path:
            base_name = os.path.basename(sat_pass.log_path)
            if "__" in base_name:
                _, rest = base_name.split("__", 1)
            else:
                rest = base_name
            rest = rest.replace("_rec.log", "").replace(".log", "")
            parts = rest.split("_")
            if len(parts) >= 2:
                date_part = parts[0]
                time_part = parts[1]
                try:
                    date_obj = datetime.strptime(date_part, "%Y%m%d").date()
                    if not params.get("pass_date"):
                        params["pass_date"] = date_obj.isoformat()
                    if not params.get("start_time"):
                        params["start_time"] = datetime.strptime(
                            f"{date_part} {time_part}", "%Y%m%d %H%M%S"
                        )
                except ValueError:
                    pass

        sat_pass.pass_id = params["pass_id"]
        sat_pass.satellite_name = params["satellite"]
        sat_pass.pass_date = params["pass_date"]
        sat_pass.pass_start_time = params["start_time"]
        sat_pass.station_name = params["station"]
        sat_pass.location = params["location"]
        sat_pass.pass_end_time = params["stop_time"]
        sat_pass.snr_sum = params["snr_sum"]
        sat_pass.snr_awg = params["snr_awg"]
        sat_pass.snr_max = params["snr_max"]
        sat_pass.rx_start_time = params["rx_start_time"]
        sat_pass.rx_end_time = params["rx_end_time"]
        sat_pass.success = params["success"]

        # Fallback: вытаскиваем дату/время из pass_id, если все еще нет.
        if (sat_pass.pass_date is None or sat_pass.pass_start_time is None) and sat_pass.pass_id:
            try:
                date_part, time_part, *_ = sat_pass.pass_id.split("_")
                if sat_pass.pass_date is None:
                    sat_pass.pass_date = datetime.strptime(date_part, "%Y%m%d").date().isoformat()
                if sat_pass.pass_start_time is None:
                    sat_pass.pass_start_time = datetime.strptime(
                        f"{date_part} {time_part}", "%Y%m%d %H%M%S"
                    )
            except (ValueError, IndexError):
                pass

        # Fallback: если есть pass_start_time, но нет pass_date.
        if sat_pass.pass_date is None and isinstance(sat_pass.pass_start_time, datetime):
            sat_pass.pass_date = sat_pass.pass_start_time.date().isoformat()

        return sat_pass


if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent
    LOG_PATH = str(BASE_DIR / "server_logs")
    LOGS_ROOT = str(BASE_DIR / "passes_logs")

    logger = Logger(path_log=LOG_PATH, log_level="debug", logger_name="ground_link_analyzer")
    analyzer = PassAnalyzer(logger=logger)

    #test 1 20260127_031121_FENGYUN_3D
    print("test 1 20260127_031121_FENGYUN_3D")
    print("--------------------------------")
    print()
    # Мини-тест аналитики на одном лог-файле, если он существует.
    sample_log = (
        BASE_DIR
        / "passes_logs"
        / "2026"
        / "01"
        / "27"
        / "R4.6S_Anadyr"
        / "R4.6S_Anadyr__20260127_031121_FENGYUN_3D_rec.log"
    )
    if sample_log.exists():

        test_pass = SatPas(log_path=str(sample_log))
        analyzed = analyzer.analyze_pass(test_pass)
        print(analyzed)

    else:

        print(f"test log not found: {sample_log}")

    #test 2 PlanumMoscow__20260127_072847_METEOR-M2_3

    print("test 2 PlanumMoscow__20260127_072847_METEOR-M2_3")
    print("--------------------------------")
    print()

    sample_log = (
        BASE_DIR
        / "passes_logs"
        / "2026"
        / "01"
        / "27"
        / "PlanumMoscow"
        / "PlanumMoscow__20260127_072847_METEOR-M2_3_rec.log"
    )
    if sample_log.exists():

        test_pass = SatPas(log_path=str(sample_log))
        analyzed = analyzer.analyze_pass(test_pass)
        print(analyzed)

    else:

        print(f"test log not found: {sample_log}")

    # test 3 R3.2S-Naryan-Mar__20260129_070456_NOAA_20_JPSS-1

    print()
    print("test 3 R3.2S-Naryan-Mar__20260129_070456_NOAA_20_JPSS-1")
    print("--------------------------------")
    print()

    sample_log = (
        BASE_DIR
        / "passes_logs"
        / "2026"
        / "01"
        / "29"
        / "R3.2S-Naryan-Mar"
        / "R3.2S-Naryan-Mar__20260129_070456_NOAA_20_JPSS-1_rec.log"
    )

    if sample_log.exists():

        test_pass = SatPas(log_path=str(sample_log))
        analyzed = analyzer.analyze_pass(test_pass)
        print(analyzed)

    else:

        print(f"test log not found: {sample_log}")

    # test 4 PlanumMoscow__20260129_082629_METOP-C

    print()
    print("test 4 PlanumMoscow__20260129_082629_METOP-C")
    print("--------------------------------")
    print()

    sample_log = (
        BASE_DIR
        / "passes_logs"
        / "2026"
        / "01"
        / "29"
        / "PlanumMoscow"
        / "PlanumMoscow__20260129_082629_METOP-C_rec.log"
    )
    if sample_log.exists():

        test_pass = SatPas(log_path=str(sample_log))
        analyzed = analyzer.analyze_pass(test_pass)
        print(analyzed) 

    else:

        print(f"test log not found: {sample_log}")

    # test 5: анализ всех лог-файлов из папки passes_logs
    print()
    print("test 5 анализ всех лог файлов из passes_logs")
    print("--------------------------------")
    print()

    all_passes = []
    for root, _, files in os.walk(LOGS_ROOT):
        for filename in files:
            if filename.lower().endswith(".log"):
                analyzer.logger.debug(f"analyze log file: {os.path.join(root, filename)}")
                all_passes.append(SatPas(log_path=os.path.join(root, filename)))
                analyzed = analyzer.analyze_pass(test_pass)
                print
                print(analyzed)
                print()