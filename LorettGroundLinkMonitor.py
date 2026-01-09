import requests
from bs4 import BeautifulSoup
import json
import re
import urllib3
import sys
import shutil
import os
import logging
import smtplib
from pathlib import Path
from urllib.parse import urljoin
from collections import defaultdict
from colorama import init, Fore, Style
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Dict, Any
import asyncio
import time
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage

# Импортируем Logger
try:
    from Logger import Logger
    LOGGER_AVAILABLE = True
except ImportError:
    LOGGER_AVAILABLE = False

# Попытка импортировать aiohttp для асинхронных запросов
try:
    import aiohttp
    import ssl
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# Константы для прогресс-бара
PROGRESS_BAR_WIDTH = 30
# Используем ASCII символы для совместимости с разными кодировками
PROGRESS_BAR_CHAR = "-"  # Можно использовать "=" или "#"

init(autoreset=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройка логирования - все логи в единый файл lorett_monitor.log
log_file_path = '/root/lorett/GroundLinkMonitorServer/lorett_monitor.log'
log_dir = os.path.dirname(log_file_path)
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file_path, encoding='utf-8', mode='a')
    ],
    force=True  # Перезаписываем предыдущую конфигурацию
)
logger = logging.getLogger(__name__)

# === Константы (загружаются из config.json с fallback на значения по умолчанию) ===
# Значения по умолчанию используются если конфиг не найден или параметры отсутствуют
_DEFAULT_CONSTANTS = {
    'min_log_file_size': 100,
    'min_avg_snr_threshold': 4.0,
    'x_bend_failure_threshold': 3.85,
    'l_bend_failure_threshold': 0.0,
    'request_timeout': 30,
    'max_concurrent_downloads': 10,
    'graph_viewport_width': 620,
    'graph_viewport_height': 680,
    'graph_display_width': 500,
    'graph_load_delay': 0.5,
    'graph_scroll_x': 0,
    'graph_scroll_y': 0
}

# Валидирует значения констант
def _validate_constants(constants: Dict[str, Any]) -> Dict[str, Any]:
    """Валидирует значения констант из конфига."""
    validated = {}
    for key, value in constants.items():
        if key.endswith('_threshold') or key.endswith('_size'):
            if not isinstance(value, (int, float)) or value < 0:
                raise ValueError(f"Константа {key} должна быть неотрицательным числом, получено: {value}")
            validated[key] = float(value) if isinstance(value, (int, float)) else value
        elif key == 'max_concurrent_downloads':
            if not isinstance(value, int) or value < 1 or value > 100:
                raise ValueError(f"{key} должна быть целым числом от 1 до 100, получено: {value}")
            validated[key] = value
        elif key.endswith('_width') or key.endswith('_height'):
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"Константа {key} должна быть положительным целым числом, получено: {value}")
            validated[key] = value
        elif key.endswith('_scroll_x') or key.endswith('_scroll_y'):
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"Константа {key} должна быть неотрицательным целым числом, получено: {value}")
            validated[key] = value
        elif key.endswith('_delay'):
            if not isinstance(value, (int, float)) or value < 0:
                raise ValueError(f"Константа {key} должна быть неотрицательным числом, получено: {value}")
            validated[key] = float(value)
        elif key == 'request_timeout':
            if not isinstance(value, (int, float)) or value < 1:
                raise ValueError(f"{key} должна быть положительным числом, получено: {value}")
            validated[key] = float(value)
        else:
            validated[key] = value
    return validated

# Загружаем константы из конфига
def _load_constants() -> Dict[str, Any]:
    """Загружает и валидирует константы из config.json или использует значения по умолчанию."""
    try:
        config_path = Path('/root/lorett/GroundLinkMonitorServer/config.json')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                constants = config.get('constants', {})
                # Объединяем с дефолтными значениями (конфиг имеет приоритет)
                result = {**_DEFAULT_CONSTANTS, **constants}
                # Валидируем значения
                result = _validate_constants(result)
                logger.debug(f"Константы загружены из config.json: {result}")
                return result
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Не удалось загрузить константы из config.json: {e}. Используются значения по умолчанию.")
    except ValueError as e:
        logger.error(f"Ошибка валидации констант: {e}. Используются значения по умолчанию.")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке констант: {e}", exc_info=True)
    return _DEFAULT_CONSTANTS.copy()

# Инициализируем константы при импорте модуля
_CONSTANTS = _load_constants()

# Экспортируем константы как переменные модуля
MIN_LOG_FILE_SIZE = _CONSTANTS['min_log_file_size']
MIN_AVG_SNR_THRESHOLD = _CONSTANTS['min_avg_snr_threshold']
X_BEND_FAILURE_THRESHOLD = _CONSTANTS['x_bend_failure_threshold']
L_BEND_FAILURE_THRESHOLD = _CONSTANTS['l_bend_failure_threshold']
REQUEST_TIMEOUT = _CONSTANTS['request_timeout']
MAX_CONCURRENT_DOWNLOADS = _CONSTANTS['max_concurrent_downloads']
GRAPH_VIEWPORT_WIDTH = _CONSTANTS['graph_viewport_width']
GRAPH_VIEWPORT_HEIGHT = _CONSTANTS['graph_viewport_height']
GRAPH_DISPLAY_WIDTH = _CONSTANTS['graph_display_width']
GRAPH_LOAD_DELAY = _CONSTANTS['graph_load_delay']
GRAPH_SCROLL_X = _CONSTANTS['graph_scroll_x']
GRAPH_SCROLL_Y = _CONSTANTS['graph_scroll_y']


# Создает SSL контекст без проверки сертификатов для асинхронных запросов
def create_unverified_ssl_context():
    """
    Создает SSL контекст с отключенной проверкой сертификатов.
    
    Returns:
        ssl.SSLContext: SSL контекст с отключенной проверкой
    """
    if not AIOHTTP_AVAILABLE:
        return None
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return ssl_context


# Строит URL для получения списка логов по типу станции (oper/reg/frames)
def build_logs_url(station_type: str, base_urls: Dict[str, str], 
                   date_before: str, date_after: str) -> str:
    """
    Строит URL для получения списка логов в зависимости от типа станции.
    
    Args:
        station_type: Тип станции ('oper', 'reg', 'frames')
        base_urls: Словарь базовых URL для типов станций
        date_before: Начальная дата в формате YYYY-MM-DD
        date_after: Конечная дата в формате YYYY-MM-DD
        
    Returns:
        str: Полный URL для получения списка логов
    """
    base_url = base_urls.get(station_type, base_urls.get('reg', 'http://eus.lorett.org/eus'))
    
    if station_type == 'reg':
        return f"{base_url}/logs_list.html?t0={date_before}&t1={date_after}"
    elif station_type == 'frames':
        return f"{base_url}/loglist_frames.html?t0={date_before}&t1={date_after}"
    else:  # oper
        return f"{base_url}/logs.html?group=*&stid=*&satlist=*&t0={date_before}&t1={date_after}"


# Возвращает пути для даты: year, month, date_str, date_display
def get_date_paths(target_date: str) -> Tuple[str, str, str, str]:
    """
    Возвращает пути для даты: year, month, date_str, date_display.
    
    Args:
        target_date: Дата в формате YYYYMMDD
        
    Returns:
        Tuple[str, str, str, str]: (year, month, date_str, date_display)
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d")
    return (
        date_obj.strftime("%Y"),
        date_obj.strftime("%m"),
        target_date,
        date_obj.strftime("%d.%m.%Y")
    )


def _load_email_defaults_from_test_email() -> Dict[str, Any]:
    """
    Пытается загрузить SMTP-настройки по умолчанию из test_email.py.
    Нужен для обратной совместимости и быстрого старта (как просили: "используя данные из test_email.py").
    """
    try:
        import test_email as te  # type: ignore
        return {
            "smtp_server": getattr(te, "SMTP_SERVER", None),
            "smtp_port": getattr(te, "SMTP_PORT", None),
            "sender_email": getattr(te, "SENDER_EMAIL", None),
            "sender_password": getattr(te, "SENDER_PASSWORD", None),
            "recipient_email": getattr(te, "RECIPIENT_EMAIL", None),
            "subject": getattr(te, "EMAIL_SUBJECT", None),
        }
    except Exception:
        return {}


def get_email_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Возвращает настройки email.

    Приоритет источников:
    - config.json -> email.*
    - переменные окружения
    - значения по умолчанию из test_email.py
    """
    defaults = _load_email_defaults_from_test_email()
    email_cfg = (config or {}).get("email", {}) if isinstance(config, dict) else {}

    enabled_raw = email_cfg.get("enabled", os.getenv("EMAIL_ENABLED"))
    if enabled_raw is None:
        enabled = True  # по умолчанию включаем (пользователь попросил сделать отправку)
    elif isinstance(enabled_raw, bool):
        enabled = enabled_raw
    else:
        enabled = str(enabled_raw).strip().lower() in ("1", "true", "yes", "y", "on")

    # Параметры SMTP
    smtp_server = (
        email_cfg.get("smtp_server")
        or os.getenv("SMTP_SERVER")
        or defaults.get("smtp_server")
        or "smtp.yandex.ru"
    )
    smtp_port = int(
        email_cfg.get("smtp_port")
        or os.getenv("SMTP_PORT")
        or defaults.get("smtp_port")
        or 465
    )
    sender_email = (
        email_cfg.get("sender_email")
        or os.getenv("SENDER_EMAIL")
        or defaults.get("sender_email")
        or ""
    )
    sender_password = (
        email_cfg.get("sender_password")
        or os.getenv("SENDER_PASSWORD")
        or defaults.get("sender_password")
        or ""
    )

    # Получатели: одна строка, можно через запятую/точку с запятой
    recipient_raw = (
        email_cfg.get("recipient_email")
        or email_cfg.get("to")
        or os.getenv("RECIPIENT_EMAIL")
        or defaults.get("recipient_email")
        or ""
    )
    recipients = [r.strip() for r in re.split(r"[;,]", str(recipient_raw)) if r.strip()]

    # CC (копия): можно строкой "a@x.ru, b@y.ru" или списком ["a@x.ru", "b@y.ru"]
    cc_raw = (
        email_cfg.get("cc")
        or email_cfg.get("cc_emails")
        or os.getenv("EMAIL_CC")
        or ""
    )
    if isinstance(cc_raw, (list, tuple, set)):
        cc_recipients = [str(r).strip() for r in cc_raw if str(r).strip()]
    else:
        cc_recipients = [r.strip() for r in re.split(r"[;,]", str(cc_raw)) if r.strip()]

    subject = (
        email_cfg.get("subject")
        or os.getenv("EMAIL_SUBJECT")
        or defaults.get("subject")
        or "Ежедневное письмо"
    )

    attach_report_raw = email_cfg.get("attach_report", os.getenv("EMAIL_ATTACH_REPORT", "1"))
    attach_report = True if attach_report_raw is None else str(attach_report_raw).strip().lower() in ("1", "true", "yes", "y", "on")

    return {
        "enabled": enabled,
        "smtp_server": smtp_server,
        "smtp_port": smtp_port,
        "sender_email": sender_email,
        "sender_password": sender_password,
        "recipients": recipients,
        "cc_recipients": cc_recipients,
        "subject": subject,
        "attach_report": attach_report,
    }


def _parse_station_summary_file(summary_path: Path) -> Optional[Tuple[int, int]]:
    """
    Парсит файл avg_snr_<station>.txt и возвращает (total_files, unsuccessful_passes).
    Формат пишется в analyze_downloaded_logs().
    """
    try:
        if not summary_path.exists():
            return None
        total_files: Optional[int] = None
        unsuccessful: Optional[int] = None
        with open(summary_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("Всего файлов обработано:"):
                    m = re.search(r"(\d+)", line)
                    if m:
                        total_files = int(m.group(1))
                # Поддерживаем оба варианта формулировки
                elif line.startswith("Неуспешных пролетов:") or line.startswith("Пустых пролетов:"):
                    m = re.search(r"(\d+)", line)
                    if m:
                        unsuccessful = int(m.group(1))
        if total_files is None or unsuccessful is None:
            return None
        return total_files, unsuccessful
    except Exception as e:
        logger.debug(f"Не удалось распарсить {summary_path}: {e}")
        return None


def _compute_overall_unsuccessful_for_date(
    *,
    date_yyyymmdd: str,
    stations: Dict[str, str],
    station_bend_map: Dict[str, str],
) -> Tuple[int, int]:
    """
    Возвращает (total_files_all, unsuccessful_all) по всем станциям за дату.
    Сначала пытается читать ранее сохраненные avg_snr_*.txt, иначе считает по логам.
    """
    year, month, date_str, _ = get_date_paths(date_yyyymmdd)
    base_logs_dir = Path("/root/lorett/GroundLinkMonitorServer/logs") / year / month / date_str
    if not base_logs_dir.exists():
        return 0, 0

    total_all = 0
    unsuccessful_all = 0

    for station_name in stations.keys():
        station_folder = base_logs_dir / station_name
        if not station_folder.exists():
            continue

        # Быстрый путь: берем из summary файла, если он есть
        summary_path = station_folder / f"avg_snr_{station_folder.name}.txt"
        parsed = _parse_station_summary_file(summary_path)
        if parsed:
            total_files, unsuccessful = parsed
            total_all += int(total_files)
            unsuccessful_all += int(unsuccessful)
            continue

        # Fallback: считаем по логам
        bend_type = station_bend_map.get(station_name)
        bend_type_upper = (bend_type or "L").upper()
        threshold = X_BEND_FAILURE_THRESHOLD if bend_type_upper == "X" else L_BEND_FAILURE_THRESHOLD

        log_files = sorted(station_folder.glob("*.log"))
        if not log_files:
            continue

        total_files = 0
        unsuccessful = 0
        for log_file in log_files:
            snr_sum, count = extract_snr_from_log(log_file, bend_type)
            avg_snr = snr_sum / count if count > 0 else 0.0
            total_files += 1
            if avg_snr <= threshold:
                unsuccessful += 1

        total_all += total_files
        unsuccessful_all += unsuccessful

    return total_all, unsuccessful_all


def generate_overall_unsuccessful_7d_chart(
    *,
    target_date: str,
    stations: Dict[str, str],
    station_bend_map: Dict[str, str],
    output_path: Path,
    days: int = 7,
) -> Optional[Path]:
    """
    Генерирует PNG график общего процента пустых пролетов за последние N дней (включая target_date).
    Возвращает путь к PNG или None (если не удалось сгенерировать).
    """
    try:
        # matplotlib может отсутствовать — тогда просто пропускаем
        import matplotlib

        matplotlib.use("Agg")  # без GUI
        import matplotlib.pyplot as plt  # type: ignore
        # Не засоряем лог INFO-сообщениями matplotlib
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

        date_obj = datetime.strptime(target_date, "%Y%m%d")
        points: List[Tuple[str, Optional[float]]] = []

        for i in range(days - 1, -1, -1):
            d = date_obj - timedelta(days=i)
            d_str = d.strftime("%Y%m%d")
            label = d.strftime("%d.%m")

            total_all, unsuccessful_all = _compute_overall_unsuccessful_for_date(
                date_yyyymmdd=d_str,
                stations=stations,
                station_bend_map=station_bend_map,
            )
            if total_all <= 0:
                points.append((label, None))
            else:
                points.append((label, (unsuccessful_all / total_all) * 100.0))

        labels = [p[0] for p in points]
        # Для линейного графика: пропуски данных делаем разрывами (NaN)
        values = [(p[1] if p[1] is not None else float("nan")) for p in points]

        fig = plt.figure(figsize=(10, 3.2), dpi=150)
        ax = fig.add_subplot(111)
        x = list(range(len(labels)))
        ax.plot(
            x,
            values,
            color="#1976d2",
            linewidth=2,
            marker="o",
            markersize=4,
        )
        ax.set_xticks(x, labels)
        ax.set_ylim(0, 100)
        ax.set_yticks(list(range(0, 101, 10)))
        ax.set_ylabel("% пустых")
        ax.set_title("Общий % пустых пролетов за последние 7 дней (все станции)")
        ax.grid(axis="y", linestyle="--", alpha=0.3)

        # Подписи значений
        for idx, (lbl, v) in enumerate(points):
            if v is None:
                ax.text(x[idx], 0.5, "нет\nданных", ha="center", va="bottom", fontsize=7, color="#616161")
            else:
                ax.text(x[idx], min(99.5, v + 1.5), f"{v:.1f}%", ha="center", va="bottom", fontsize=7, color="#212121")

        fig.tight_layout()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, bbox_inches="tight")
        plt.close(fig)
        return output_path
    except ImportError:
        logger.warning("matplotlib не установлен — сводный график за 7 дней не будет добавлен в письмо")
        return None
    except Exception as e:
        logger.warning(f"Не удалось сгенерировать сводный график за 7 дней: {e}", exc_info=True)
        return None


def build_stats_email_body(
    target_date: str,
    all_results: Dict[str, Dict[str, Any]],
    graphs_dir: Optional[Path] = None,
    summary_7d_chart_path: Optional[Path] = None,
) -> Tuple[str, Dict[str, Path]]:
    """
    Формирует HTML таблицу статистики для письма и собирает графики для встраивания.
    
    Returns:
        Tuple[str, Dict[str, Path]]: (HTML тело письма, словарь {cid: путь_к_графику})
    """
    # Форматируем дату в формат DD.MM.YYYY
    date_display = f"{target_date[6:8]}.{target_date[4:6]}.{target_date[0:4]}"
    
    # Начинаем HTML документ
    html_lines = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1.0'>",
        "<style>",
        "  * { box-sizing: border-box; }",
        "  body {",
        "    font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'SF Pro Text', 'Helvetica Neue', Helvetica, Arial, sans-serif;",
        "    font-size: 15px;",
        "    line-height: 1.6;",
        "    color: #1d1d1f;",
        "    background-color: #f5f5f7;",
        "    margin: 0;",
        "    padding: 12px 8px;",
        "    -webkit-text-size-adjust: 100%;",
        "    -ms-text-size-adjust: 100%;",
        "  }",
        "  .container {",
        "    max-width: 900px;",
        "    margin: 0 auto;",
        "    background-color: #ffffff;",
        "    border-radius: 12px;",
        "    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);",
        "    overflow: hidden;",
        "  }",
        "  .header {",
        "    padding: 20px 16px 14px;",
        "    border-bottom: 1px solid #e5e5e7;",
        "    background: linear-gradient(to bottom, #ffffff, #fafafa);",
        "  }",
        "  h2 {",
        "    font-size: 20px;",
        "    font-weight: 600;",
        "    letter-spacing: -0.5px;",
        "    color: #1d1d1f;",
        "    margin: 0 0 8px 0;",
        "  }",
        "  .date {",
        "    font-size: 17px;",
        "    color: #86868b;",
        "    font-weight: 400;",
        "    margin: 0;",
        "  }",
        "  .content {",
        "    padding: 12px;",
        "  }",
        "  .table-wrap {",
        "    width: 100%;",
        "    overflow-x: auto;",
        "    -webkit-overflow-scrolling: touch;",
        "  }",
        "  .adaptive-table {",
        "    width: 100%;",
        "    min-width: 600px;",
        "    border-collapse: separate;",
        "    border-spacing: 0;",
        "    background-color: #ffffff;",
        "    border-radius: 12px;",
        "    overflow: hidden;",
        "    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);",
        "  }",
        "  .adaptive-table thead { background-color: #f5f5f7; }",
        "  .adaptive-table th {",
        "    padding: 16px 20px;",
        "    text-align: left;",
        "    font-size: 13px;",
        "    font-weight: 600;",
        "    color: #86868b;",
        "    text-transform: uppercase;",
        "    letter-spacing: 0.5px;",
        "    border-bottom: 1px solid #e5e5e7;",
        "    border-right: 1px solid #e5e5e7;",
        "    white-space: nowrap;",
        "  }",
        "  .adaptive-table th:last-child { border-right: none; }",
        "  .adaptive-table th.number { text-align: right; }",
        "  .adaptive-table td {",
        "    padding: 16px 20px;",
        "    border-bottom: 1px solid #f5f5f7;",
        "    border-right: 1px solid #e5e5e7;",
        "    font-size: 15px;",
        "    color: #1d1d1f;",
        "    white-space: nowrap;",
        "  }",
        "  .adaptive-table td:last-child { border-right: none; }",
        "  .adaptive-table tr:last-child td { border-bottom: none; }",
        "  .adaptive-table tr:hover { background-color: #fafafa; }",
        "  .adaptive-table .number { text-align: right; font-variant-numeric: tabular-nums; }",
        "  .adaptive-table .total-row { background-color: #f5f5f7; font-weight: 600; }",
        "  .adaptive-table .total-row td { border-top: 2px solid #e5e5e7; }",
        "  .adaptive-table .row-good { background-color: #dcfce7; }",
        "  .adaptive-table .row-warning { background-color: #fef3c7; }",
        "  .adaptive-table .row-error { background-color: #fee2e2; }",
        "  .summary-table {",
        "    width: 100%;",
        "    border-collapse: separate;",
        "    border-spacing: 0;",
        "    background-color: #ffffff;",
        "    border-radius: 12px;",
        "    overflow: hidden;",
        "    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);",
        "  }",
        "  .summary-table thead {",
        "    background-color: #f5f5f7;",
        "  }",
        "  .summary-table th {",
        "    padding: 16px 20px;",
        "    text-align: left;",
        "    font-size: 13px;",
        "    font-weight: 600;",
        "    color: #86868b;",
        "    text-transform: uppercase;",
        "    letter-spacing: 0.5px;",
        "    border-bottom: 1px solid #e5e5e7;",
        "    border-right: 1px solid #e5e5e7;",
        "  }",
        "  .summary-table th:last-child { border-right: none; }",
        "  .summary-table th.number { text-align: right; }",
        "  .summary-table td {",
        "    padding: 16px 20px;",
        "    border-bottom: 1px solid #f5f5f7;",
        "    border-right: 1px solid #e5e5e7;",
        "    font-size: 15px;",
        "    color: #1d1d1f;",
        "  }",
        "  .summary-table td:last-child { border-right: none; }",
        "  .summary-table tr:last-child td { border-bottom: none; }",
        "  .summary-table tr:hover {",
        "    background-color: #fafafa;",
        "  }",
        "  .summary-table .row-good {",
        "    background-color: #dcfce7;",
        "  }",
        "  .summary-table .row-good:hover {",
        "    background-color: #dcfce7;",
        "  }",
        "  .summary-table .row-warning {",
        "    background-color: #fef3c7;",
        "  }",
        "  .summary-table .row-warning:hover {",
        "    background-color: #fef3c7;",
        "  }",
        "  .summary-table .row-error {",
        "    background-color: #fee2e2;",
        "  }",
        "  .summary-table .row-error:hover {",
        "    background-color: #fee2e2;",
        "  }",
        "  .summary-table .number {",
        "    text-align: right;",
        "    font-variant-numeric: tabular-nums;",
        "  }",
        "  .summary-table .total-row {",
        "    background-color: #f5f5f7;",
        "    font-weight: 600;",
        "  }",
        "  .summary-table .total-row td {",
        "    border-top: 2px solid #e5e5e7;",
        "    padding-top: 20px;",
        "    padding-bottom: 20px;",
        "  }",
        "  /* Вертикальная 'карточка' станции внутри одной ячейки */",
        "  .station-name {",
        "    font-weight: 600;",
        "    font-size: 15px;",
        "    margin: 0 0 8px 0;",
        "  }",
        "  .metrics-table {",
        "    width: 100%;",
        "    border-collapse: collapse;",
        "  }",
        "  .metrics-table td {",
        "    padding: 6px 0;",
        "    border: none;",
        "    border-bottom: 1px solid #f0f0f2;",
        "    font-size: 14px;",
        "  }",
        "  .metrics-table tr:last-child td { border-bottom: none; }",
        "  .metrics-label {",
        "    color: #86868b;",
        "    font-size: 11px;",
        "    font-weight: 600;",
        "    text-transform: uppercase;",
        "    letter-spacing: 0.5px;",
        "  }",
        "  .metrics-value {",
        "    text-align: right;",
        "    font-variant-numeric: tabular-nums;",
        "  }",
        "",
        "  /* Десктоп-таблица (изначальная, 5 колонок) */",
        "  .desktop-table {",
        "    width: 100%;",
        "    border-collapse: separate;",
        "    border-spacing: 0;",
        "    background-color: #ffffff;",
        "    border-radius: 12px;",
        "    overflow: hidden;",
        "    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);",
        "  }",
        "  .desktop-table thead { background-color: #f5f5f7; }",
        "  .desktop-table th {",
        "    padding: 16px 20px;",
        "    text-align: left;",
        "    font-size: 13px;",
        "    font-weight: 600;",
        "    color: #86868b;",
        "    text-transform: uppercase;",
        "    letter-spacing: 0.5px;",
        "    border-bottom: 1px solid #e5e5e7;",
        "    border-right: 1px solid #e5e5e7;",
        "  }",
        "  .desktop-table th:last-child { border-right: none; }",
        "  .desktop-table th.number { text-align: right; }",
        "  .desktop-table td {",
        "    padding: 16px 20px;",
        "    border-bottom: 1px solid #f5f5f7;",
        "    border-right: 1px solid #e5e5e7;",
        "    font-size: 15px;",
        "    color: #1d1d1f;",
        "  }",
        "  .desktop-table td:last-child { border-right: none; }",
        "  .desktop-table tr:last-child td { border-bottom: none; }",
        "  .desktop-table tr:hover { background-color: #fafafa; }",
        "  .desktop-table .number { text-align: right; font-variant-numeric: tabular-nums; }",
        "  .desktop-table .total-row { background-color: #f5f5f7; font-weight: 600; }",
        "  .desktop-table .total-row td { border-top: 2px solid #e5e5e7; }",
        "  .desktop-table .row-good { background-color: #dcfce7; }",
        "  .desktop-table .row-warning { background-color: #fef3c7; }",
        "  .desktop-table .row-error { background-color: #fee2e2; }",
        "  .graph-section {",
        "    margin-top: 24px;",
        "    padding: 10px 8px;",
        "    background-color: #fafafa;",
        "    border-radius: 12px;",
        "    page-break-inside: avoid;",
        "  }",
        "  .graph-title {",
        "    font-size: 20px;",
        "    font-weight: 600;",
        "    letter-spacing: -0.3px;",
        "    color: #1d1d1f;",
        "    margin-bottom: 20px;",
        "  }",
        "  .graph-image {",
        "    max-width: 100%;",
        "    height: auto;",
        "    border-radius: 8px;",
        "    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);",
        "    display: block;",
        "  }",
        "  .empty-message {",
        "    color: #86868b;",
        "    font-style: italic;",
        "    font-size: 14px;",
        "    padding: 16px 0;",
        "  }",
        "  .unsuccessful-list {",
        "    margin-top: 20px;",
        "    padding: 20px;",
        "    background-color: #fff5f5;",
        "    border-radius: 8px;",
        "    border-left: 3px solid #ff3b30;",
        "  }",
        "  .unsuccessful-list strong {",
        "    color: #ff3b30;",
        "    font-size: 17px;",
        "    font-weight: 600;",
        "    display: block;",
        "    margin-bottom: 12px;",
        "  }",
        "  .unsuccessful-list ul {",
        "    margin: 0;",
        "    padding-left: 20px;",
        "    color: #1d1d1f;",
        "    font-size: 16px;",
        "  }",
        "  .unsuccessful-list li {",
        "    margin-bottom: 6px;",
        "  }",
        "  .chart-container {",
        "    margin-top: 32px;",
        "    padding: 10px 8px;",
        "    background-color: #fafafa;",
        "    border-radius: 12px;",
        "  }",
        "  /* Адаптивные отступы (без media-query, используем фиксированные значения) */",
        "  body { padding: 12px 8px; }",
        "  .container { border-radius: 12px; }",
        "  .header { padding: 20px 16px 14px; }",
        "  .content { padding: 12px; }",
        "  .graph-section { padding: 10px 8px; }",
        "  .chart-container { padding: 10px 8px; }",
        "</style>",
        "</head>",
        "<body>",
        "<div class='container'>",
        "  <div class='header'>",
        f"    <h2>Сводка по станциям {date_display}</h2>",
        "  </div>",
        "  <div class='content'>",
        "    <div class='table-wrap'>",
        "      <table class='adaptive-table'>",
        "        <thead>",
        "          <tr>",
        "            <th>Станция</th>",
        "            <th class='number'>Всего</th>",
        "            <th class='number'>Успешных</th>",
        "            <th class='number'>Пустых</th>",
        "            <th class='number'>% пустых</th>",
        "          </tr>",
        "        </thead>",
        "        <tbody>"
    ]

    # Словарь для хранения графиков: {cid: путь_к_файлу}
    inline_images = {}

    # Сортируем станции по среднему SNR (как в консоли)
    sorted_stations = sorted(all_results.items(), key=lambda x: x[1].get('avg_snr', 0), reverse=True)

    for station_name, stats in sorted_stations:
        files = int(stats.get("files", 0) or 0)
        successful = int(stats.get("successful_passes", 0) or 0)
        unsuccessful = int(stats.get("unsuccessful_passes", 0) or 0)
        unsuccessful_percent = (unsuccessful / files * 100) if files > 0 else 0.0
        
        # Определяем класс строки для цветовой подсветки
        if files == 0:
            row_class = "row-error"  # Нет пролетов - красный
        elif unsuccessful_percent < 2:
            row_class = "row-good"  # 0-2% - зеленый
        elif unsuccessful_percent < 20:
            row_class = "row-warning"  # 2-20% - желтый
        else:
            row_class = "row-error"  # 20% и более - красный
        
        # Экранируем HTML специальные символы
        station_name_escaped = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        
        html_lines.append(f"        <tr class='{row_class}'>")
        html_lines.append(f"          <td>{station_name_escaped}</td>")
        html_lines.append(f"          <td class='number'>{files}</td>")
        html_lines.append(f"          <td class='number'>{successful}</td>")
        html_lines.append(f"          <td class='number'>{unsuccessful}</td>")
        html_lines.append(f"          <td class='number'>{unsuccessful_percent:.1f}%</td>")
        html_lines.append("        </tr>")

    # Итоговые значения
    total_all_files = sum(int(s.get("files", 0) or 0) for s in all_results.values())
    total_successful = sum(int(s.get("successful_passes", 0) or 0) for s in all_results.values())
    total_unsuccessful = sum(int(s.get("unsuccessful_passes", 0) or 0) for s in all_results.values())
    total_unsuccessful_percent = (total_unsuccessful / total_all_files * 100) if total_all_files > 0 else 0.0

    # Добавляем итоговую строку
    html_lines.append("        <tr class='total-row'>")
    html_lines.append("          <td>ИТОГО</td>")
    html_lines.append(f"          <td class='number'>{total_all_files}</td>")
    html_lines.append(f"          <td class='number'>{total_successful}</td>")
    html_lines.append(f"          <td class='number'>{total_unsuccessful}</td>")
    html_lines.append(f"          <td class='number'>{total_unsuccessful_percent:.1f}%</td>")
    html_lines.append("        </tr>")

    # Закрываем таблицу
    html_lines.extend([
        "        </tbody>",
        "      </table>",
        "    </div>",
    ])

    if summary_7d_chart_path and Path(summary_7d_chart_path).exists():
        # Fallback на PNG, если точки не передали
        summary_cid = "summary_unsuccessful_7d"
        inline_images[summary_cid] = Path(summary_7d_chart_path)
        html_lines.append("    <div class='chart-container'>")
        html_lines.append(f"      <img src='cid:{summary_cid}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='Сводный график за 7 дней' />")
        html_lines.append("    </div>")
    else:
        html_lines.append("    <p class='empty-message'>Нет данных для построения графика.</p>")
    
    # Добавляем графики после таблицы
    if graphs_dir and graphs_dir.exists():
        html_lines.append("    <h2 style='margin-top: 48px; font-size: 24px; font-weight: 600; letter-spacing: -0.3px; color: #1d1d1f;'>Графики пролетов</h2>")
        
        for station_name, stats in sorted_stations:
            # Экранируем HTML специальные символы
            station_name_escaped = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            
            html_lines.append(f"    <div class='graph-section'>")
            html_lines.append(f"      <div class='graph-title'>{station_name_escaped}</div>")
            
            max_snr_filename = stats.get('max_snr_filename', '')
            if max_snr_filename:
                # Формируем имя файла графика
                graph_name = max_snr_filename.replace('.log', '.png').replace(' ', '_')
                graph_path = graphs_dir / graph_name
                
                if graph_path.exists():
                    # Создаем уникальный CID для встроенного изображения
                    cid = f"graph_{station_name}_{graph_name}".replace(' ', '_').replace('.', '_')
                    inline_images[cid] = graph_path
                    
                    html_lines.append(f"      <img src='cid:{cid}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='График для {station_name_escaped}' />")
                else:
                    html_lines.append(f"      <p class='empty-message'>График не найден. Станция не работает</p>")
            else:
                html_lines.append(f"      <p class='empty-message'>График не найден. Станция не работает</p>")
            
            # Добавляем список пустых пролетов, если они есть
            unsuccessful_filenames = stats.get('unsuccessful_filenames', [])
            if unsuccessful_filenames:
                html_lines.append(f"      <div class='unsuccessful-list'>")
                html_lines.append(f"        <strong>Пустые пролеты ({len(unsuccessful_filenames)})</strong>")
                html_lines.append(f"        <ul>")
                for filename in unsuccessful_filenames:
                    # Экранируем HTML специальные символы в имени файла
                    filename_escaped = filename.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    html_lines.append(f"          <li>{filename_escaped}</li>")
                html_lines.append(f"        </ul>")
                html_lines.append(f"      </div>")
            
            html_lines.append(f"    </div>")
    
    # Закрываем HTML
    html_lines.extend([
        "  </div>",
        "</div>",
        "</body>",
        "</html>"
    ])

    return "\n".join(html_lines), inline_images


def send_stats_email(
    *,
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: str,
    recipients: List[str],
    cc_recipients: Optional[List[str]] = None,
    subject: str,
    body: str,
    attachments: Optional[List[Path]] = None,
    inline_images: Optional[Dict[str, Path]] = None,
) -> bool:
    """
    Отправляет письмо со статистикой через SMTP (SSL на 465, STARTTLS на 587).
    
    Args:
        inline_images: Словарь {cid: путь_к_изображению} для встроенных изображений
    """
    if not sender_email or not sender_password or not recipients:
        logger.warning("Email: не заданы sender/password/recipients — отправка пропущена")
        return False

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipients)
    if cc_recipients:
        msg["Cc"] = ", ".join([r for r in cc_recipients if r])
    msg["Subject"] = subject
    # Определяем, является ли body HTML (проверяем наличие HTML тегов)
    is_html = body.strip().startswith("<!DOCTYPE html>") or body.strip().startswith("<html>")
    msg.attach(MIMEText(body, "html" if is_html else "plain", "utf-8"))

    # Добавляем встроенные изображения (inline attachments)
    if inline_images:
        for cid, image_path in inline_images.items():
            try:
                if not image_path or not Path(image_path).exists():
                    logger.warning(f"Email: график не найден {image_path}")
                    continue
                with open(image_path, "rb") as f:
                    img = MIMEImage(f.read())
                img.add_header('Content-ID', f'<{cid}>')
                img.add_header('Content-Disposition', 'inline', filename=Path(image_path).name)
                msg.attach(img)
            except Exception as e:
                logger.warning(f"Email: не удалось приложить график {image_path}: {e}")

    # Добавляем обычные вложения
    for p in attachments or []:
        try:
            if not p or not Path(p).exists():
                continue
            with open(p, "rb") as f:
                part = MIMEApplication(f.read(), Name=Path(p).name)
            part["Content-Disposition"] = f'attachment; filename="{Path(p).name}"'
            msg.attach(part)
        except Exception as e:
            logger.warning(f"Email: не удалось приложить файл {p}: {e}")

    try:
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port), timeout=30)
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port), timeout=30)
            if int(smtp_port) == 587:
                server.starttls()
        server.login(sender_email, sender_password)
        # Важно: Cc должны попасть и в заголовок, и в список реальных получателей SMTP
        all_recipients: List[str] = list(recipients or [])
        if cc_recipients:
            all_recipients.extend([r for r in cc_recipients if r])
        server.send_message(msg, from_addr=sender_email, to_addrs=all_recipients)
        server.quit()
        return True
    except Exception as e:
        logger.warning(f"Email: ошибка отправки: {e}", exc_info=True)
        return False


# Выводит сообщение об ошибке в консоль с цветным форматированием и логированием
def print_error(message: str, is_critical: bool = False, exc_info: bool = False) -> None:
    """
    Выводит сообщение об ошибке в едином формате и логирует его.
    
    Args:
        message: Текст сообщения об ошибке
        is_critical: Если True, сообщение выводится с ярким стилем и логируется как ERROR
        exc_info: Если True, логирует полный traceback
    """
    style = Fore.RED + (Style.BRIGHT if is_critical else Style.NORMAL)
    print(f"{style}{message}{Style.RESET_ALL}")
    
    # Логируем ошибку
    if is_critical:
        logger.error(message, exc_info=exc_info)
    else:
        logger.warning(message, exc_info=exc_info)


# Обновляет визуальный прогресс-бар загрузки файлов
def update_progress_bar(current: int, total: int) -> None:
    """
    Обновляет прогресс-бар с безопасным выводом для разных кодировок.
    
    Args:
        current: Текущее значение прогресса
        total: Общее значение прогресса
    """
    if total == 0:
        return
    
    percentage = int(100 * current / total)
    filled = int(PROGRESS_BAR_WIDTH * current / total)
    bar = PROGRESS_BAR_CHAR * filled + " " * (PROGRESS_BAR_WIDTH - filled)
    
    try:
        sys.stdout.write(f"\r{bar} {percentage:3d}%")
        sys.stdout.flush()
    except (UnicodeEncodeError, AttributeError):
        # Fallback для консолей с проблемами кодировки
        sys.stdout.write(f"\rProgress: {percentage:3d}%")
        sys.stdout.flush()

# Проверяет валидность лог-файла (отсеивает ошибки БД и пустые файлы)
def validate_log_file_detailed(file_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Проверяет, является ли файл валидным лог-файлом и возвращает детали ошибки.
    
    Args:
        file_path: Путь к файлу для проверки
        
    Returns:
        Tuple[bool, Optional[str]]: (True, None) если файл валиден, (False, описание_ошибки) если содержит ошибки
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read().strip()
            
        # Проверка на ошибки базы данных
        if content.startswith('ERROR: No log') and 'in the database' in content:
            # Извлекаем полное сообщение об ошибке
            error_lines = content.split('\n')[:3]  # Первые 3 строки для контекста
            error_msg = ' '.join(error_lines).strip()
            return False, f"Ошибка базы данных: {error_msg}"
            
        # Проверка на другие ошибки (маленький файл с ERROR)
        if len(content) < MIN_LOG_FILE_SIZE and ('ERROR' in content.upper() or 'error' in content.lower()):
            # Извлекаем строки с ошибками
            error_lines = [line for line in content.split('\n') if 'error' in line.lower()][:3]
            error_msg = ' | '.join(error_lines).strip() if error_lines else content[:200]
            return False, f"Файл содержит ошибку (размер {len(content)} байт): {error_msg}"
        
        # Проверка на пустой файл
        if len(content) == 0:
            return False, "Файл пустой"
            
        return True, None
    except (FileNotFoundError, PermissionError, IOError, OSError) as e:
        # Если не удалось прочитать файл, считаем его валидным (консервативный подход)
        logger.debug(f"Не удалось прочитать файл {file_path} для валидации: {e}")
        return True, None


def is_valid_log_file(file_path: Path) -> bool:
    """
    Проверяет, является ли файл валидным лог-файлом.
    
    Args:
        file_path: Путь к файлу для проверки
        
    Returns:
        bool: True если файл валиден, False если содержит ошибки
    """
    is_valid, _ = validate_log_file_detailed(file_path)
    return is_valid


def is_log_file_downloaded(file_path: Path) -> bool:
    """
    Проверяет, что лог-файл успешно загружен и валиден.
    
    Args:
        file_path: Путь к файлу для проверки
        
    Returns:
        bool: True если файл загружен и валиден, False в противном случае
    """
    try:
        # Проверяем существование файла
        if not file_path.exists():
            return False
        
        # Проверяем, что файл не пустой
        if file_path.stat().st_size == 0:
            return False
        
        # Проверяем валидность содержимого
        if not is_valid_log_file(file_path):
            return False
        
        return True
    except Exception:
        return False


# Загружает конфигурацию из config.json и возвращает станции, URL и заголовки
def load_config() -> Tuple[Dict[str, str], List[str], List[str], List[str], Dict[str, str], Dict[str, str]]:
    """
    Загружает конфигурацию из файла config.json.
    
    Returns:
        Tuple содержащий:
        - Словарь {имя_станции: тип_станции}
        - Список оперативных станций
        - Список регулярных станций
        - Список frames станций
        - Словарь базовых URL для типов станций
        - Словарь HTTP заголовков
        
    Raises:
        FileNotFoundError: Если файл config.json не найден
        ValueError: Если файл содержит невалидный JSON или некорректную структуру
        RuntimeError: При других ошибках загрузки конфигурации
    """
    config_path = Path('/root/lorett/GroundLinkMonitorServer/config.json')
    
    if not config_path.exists():
        raise FileNotFoundError(f"Конфигурационный файл {config_path} не найден")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Ошибка парсинга JSON в config.json: {e}")
    except Exception as e:
        raise RuntimeError(f"Ошибка чтения файла конфигурации: {e}")
    
    try:
        stations = {s['name']: s['type'] for s in config['stations']}
        base_urls = config.get('base_urls', {
            'oper': "https://eus.lorett.org/eus",
            'reg': "http://eus.lorett.org/eus",
            'frames': "https://eus.lorett.org/eus"
        })
        headers = config.get('headers', {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        oper_stations = [n for n, t in stations.items() if t == 'oper']
        reg_stations = [n for n, t in stations.items() if t == 'reg']
        frames_stations = [n for n, t in stations.items() if t == 'frames']
        
        return stations, oper_stations, reg_stations, frames_stations, base_urls, headers
    except KeyError as e:
        raise ValueError(f"Отсутствует обязательное поле в конфигурации: {e}")
    except Exception as e:
        raise RuntimeError(f"Ошибка обработки конфигурации: {e}")


# Синхронно загружает HTML страницу со списком логов для типа станций
def fetch_logs_page(url: str, st: str, headers: Dict[str, str], max_retries: int = 3) -> str:
    """
    Загружает HTML страницу со списком логов для указанного типа станций.
    Автоматически повторяет запрос при ошибке 503 (Service Unavailable).
    
    Args:
        url: URL страницы со списком логов
        st: Тип станций ('oper', 'reg', 'frames')
        headers: HTTP заголовки для запроса
        max_retries: Максимальное количество попыток (по умолчанию 3)
        
    Returns:
        str: HTML содержимое страницы
        
    Raises:
        requests.RequestException: При ошибках HTTP запроса после всех попыток
    """
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
            r.raise_for_status()
            return r.text
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 503:
                last_error = e
                # Экспоненциальная задержка: 2, 4, 8 секунд
                delay = 2 ** attempt
                logger.warning(f"Ошибка 503 (Service Unavailable) для '{st}', попытка {attempt}/{max_retries}. Повтор через {delay} сек...")
                if attempt < max_retries:
                    time.sleep(delay)
                    continue
                logger.error(f"Превышено количество попыток для '{st}' после {max_retries} попыток")
                raise
            else:
                # Другие HTTP ошибки пробрасываем сразу
                raise
        except requests.RequestException as e:
            # Сетевые ошибки (timeout, connection error) - пробрасываем сразу
            raise
    
    # Если дошли сюда, значит все попытки исчерпаны
    if last_error:
        raise last_error
    raise requests.RequestException(f"Не удалось загрузить страницу для '{st}' после {max_retries} попыток")


# Парсит HTML и находит все логи для указанной даты, группируя по станциям
def find_logs_in_page(html: str, date: str, names: List[str], source_base_url: str) -> Dict[str, List[Tuple[str, str]]]:
    """
    Находит все логи для указанной даты на странице HTML.
    
    Args:
        html: HTML содержимое страницы со списком логов
        date: Дата в формате YYYYMMDD
        names: Список имен станций для фильтрации
        source_base_url: Базовый URL страницы, на которой были найдены логи
        
    Returns:
        Dict[str, List[Tuple[str, str]]]: Словарь {имя_станции: [(имя_файла, base_url), ...]}
    """
    log_urls = set(re.findall(rf'log_view/([^"\s<>]+__{date}[^"\s<>]+_rec\.log)', html))
    for link in BeautifulSoup(html, 'html.parser').find_all('a', href=True):
        h = link.get('href', '')
        if date in h and '_rec.log' in h:
            m = re.search(rf'([^/]+__{date}[^"\s<>]+_rec\.log)', h)
            if m:
                log_urls.add(m.group(1))
    logs_by_station = defaultdict(list)
    station_set = set(names)
    for log_url in sorted(log_urls):
        m = re.match(rf'^([^_]+(?:_[^_]+)*)__{date}', log_url)
        if m and m.group(1) in station_set:
            # Сохраняем имя файла вместе с base_url источника
            logs_by_station[m.group(1)].append((log_url, source_base_url))
    return logs_by_station


# Асинхронно скачивает один лог-файл с валидацией и ограничением параллелизма через семафор
async def download_single_log_async(
    session: 'aiohttp.ClientSession',
    log_file: str,
    file_path: Path,
    base_url: str,
    headers: Dict[str, str],
    semaphore: asyncio.Semaphore,
    max_retries: int = 2
) -> Tuple[bool, Optional[str], int]:
    """
    Асинхронно скачивает один лог-файл с повторными попытками.
    
    Args:
        session: aiohttp клиентская сессия
        log_file: Имя файла лога для скачивания
        file_path: Путь для сохранения файла
        base_url: Базовый URL для запроса
        headers: HTTP заголовки
        semaphore: Семафор для ограничения количества одновременных запросов
        max_retries: Максимальное количество попыток загрузки
        
    Returns:
        Tuple[bool, Optional[str], int]: (успешно ли скачан, сообщение об ошибке если есть, размер файла)
    """
    async with semaphore:
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                ssl_context = create_unverified_ssl_context()
                url = urljoin(base_url, f"/eus/log_get/{log_file}")
                timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                
                async with session.get(url, headers=headers, timeout=timeout, ssl=ssl_context) as response:
                    response.raise_for_status()
                    content = await response.read()
                    content_size = len(content)
                    
                    # Сохраняем содержимое во временный файл для проверки
                    temp_file = file_path.with_suffix('.tmp')
                    temp_file.write_bytes(content)
                    
                    # Проверяем содержимое файла на наличие ошибок
                    is_valid, error_detail = validate_log_file_detailed(temp_file)
                    if not is_valid:
                        # Файл содержит ошибку - не сохраняем его
                        try:
                            temp_file.unlink()
                        except (FileNotFoundError, PermissionError) as e:
                            logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")
                        last_error = error_detail if error_detail else "Лог содержит ошибки"
                        if attempt < max_retries:
                            await asyncio.sleep(1)  # Небольшая задержка перед повтором
                            continue
                        return False, last_error, 0
                    
                    # Файл валиден - переименовываем временный файл в постоянный
                    try:
                        temp_file.replace(file_path)
                    except (OSError, PermissionError) as e:
                        logger.error(f"Не удалось переместить файл {temp_file} в {file_path}: {e}")
                        last_error = f"Ошибка сохранения файла: {e}"
                        if attempt < max_retries:
                            await asyncio.sleep(1)
                            continue
                        return False, last_error, 0
                    
                    # Проверяем, что файл действительно загружен
                    if is_log_file_downloaded(file_path):
                        return True, None, content_size
                    else:
                        last_error = "Файл не прошел проверку после загрузки"
                        if attempt < max_retries:
                            logger.warning(f"Попытка {attempt}/{max_retries} загрузки {log_file} не удалась: {last_error}")
                            await asyncio.sleep(1)
                            continue
                        return False, last_error, 0
                    
            except asyncio.TimeoutError:
                last_error = "Таймаут запроса"
                if attempt < max_retries:
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {log_file}: {last_error}")
                    await asyncio.sleep(1)
                    continue
                return False, last_error, 0
            except aiohttp.ClientResponseError as e:
                # Специальная обработка для 503 Service Unavailable
                if e.status == 503:
                    delay = 2 ** attempt  # Экспоненциальная задержка: 2, 4, 8 секунд
                    last_error = f"Ошибка 503 (Service Unavailable)"
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {log_file}: {last_error}. Повтор через {delay} сек...")
                    if attempt < max_retries:
                        await asyncio.sleep(delay)
                        continue
                    return False, last_error, 0
                else:
                    last_error = f"Ошибка HTTP {e.status}: {e.message}"
                    if attempt < max_retries:
                        logger.warning(f"Попытка {attempt}/{max_retries} загрузки {log_file}: {last_error}")
                        await asyncio.sleep(1)
                        continue
                    return False, last_error, 0
            except aiohttp.ClientError as e:
                last_error = f"Ошибка HTTP: {e}"
                if attempt < max_retries:
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {log_file}: {last_error}")
                    await asyncio.sleep(1)
                    continue
                return False, last_error, 0
            except (OSError, PermissionError) as e:
                last_error = f"Ошибка файловой системы: {e}"
                if attempt < max_retries:
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {log_file}: {last_error}")
                    await asyncio.sleep(1)
                    continue
                return False, last_error, 0
            except Exception as e:
                last_error = f"Неожиданная ошибка: {e}"
                if attempt < max_retries:
                    logger.warning(f"Попытка {attempt}/{max_retries} загрузки {log_file}: {last_error}")
                    await asyncio.sleep(1)
                    continue
                return False, last_error, 0
        
        return False, last_error or "Превышено максимальное количество попыток", 0


# Асинхронно скачивает все логи параллельно (до MAX_CONCURRENT_DOWNLOADS одновременно) с прогресс-баром
async def download_logs_async(
    all_logs: Dict[str, List[Tuple[str, str]]],
    stations: Dict[str, str],
    base_urls: Dict[str, str],
    headers: Dict[str, str],
    logs_dir: Path
) -> Tuple[int, int]:
    """
    Асинхронно скачивает все логи из словаря all_logs.
    
    Args:
        all_logs: Словарь {имя_станции: [(имя_лог_файла, base_url), ...]}
        stations: Словарь {имя_станции: тип_станции}
        base_urls: Словарь базовых URL для типов станций (не используется, оставлен для совместимости)
        headers: HTTP заголовки
        logs_dir: Базовая директория для сохранения логов
        
    Returns:
        Tuple[int, int]: (количество скачанных файлов, количество ошибок)
    """
    if not AIOHTTP_AVAILABLE:
        print_error("aiohttp не установлен. Установите: pip install aiohttp", is_critical=True)
        return 0, 0
    
    downloaded = 0
    failed = 0
    
    ssl_context = create_unverified_ssl_context()
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    
    # Создаем семафор для ограничения количества одновременных запросов
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    # Подготавливаем список всех файлов для загрузки
    files_to_download = []
    existing_files_count = 0
    
    for station_name, log_files_with_urls in sorted(all_logs.items()):
        # Проверка существования станции в конфиге
        if station_name not in stations:
            continue
        
        station_dir = logs_dir / station_name
        station_dir.mkdir(exist_ok=True)
        
        for log_file, source_base_url in sorted(log_files_with_urls):
            file_path = station_dir / log_file
            
            # Проверяем существующий файл
            if file_path.exists():
                if is_valid_log_file(file_path):
                    existing_files_count += 1
                    continue
                else:
                    # Файл содержит ошибку - удаляем его
                    try:
                        file_path.unlink()
                    except (FileNotFoundError, PermissionError) as e:
                        logger.warning(f"Не удалось удалить невалидный файл {file_path}: {e}")
            
            # Используем base_url источника, на котором был найден лог
            files_to_download.append((log_file, file_path, source_base_url))
    
    total_to_download = len(files_to_download)
    total_files = total_to_download + existing_files_count
    
    # Выводим информацию о загрузке перед прогресс-баром
    if existing_files_count > 0 or total_to_download > 0:
        print(f"{Fore.CYAN}Загрузка: {existing_files_count} файлов уже существует, {total_to_download} файлов к загрузке")
    
    # Инициализируем прогресс-бар
    current_progress = existing_files_count
    progress_lock = asyncio.Lock()
    if total_files > 0:
        update_progress_bar(current_progress, total_files)
    
    async def download_with_progress(session: 'aiohttp.ClientSession', log_file: str, file_path: Path, 
                                     base_url: str, headers: Dict[str, str], semaphore: asyncio.Semaphore) -> Tuple[bool, Optional[str], int]:
        """Обертка для обновления прогресс-бара"""
        nonlocal current_progress
        result = await download_single_log_async(session, log_file, file_path, base_url, headers, semaphore)
        async with progress_lock:
            current_progress += 1
            if total_files > 0:
                update_progress_bar(current_progress, total_files)
        return result
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Выполняем все задачи параллельно
        if files_to_download:
            # Создаем задачи с сессией
            tasks = [
                download_with_progress(session, log_file, file_path, base_url, headers, semaphore)
                for log_file, file_path, base_url in files_to_download
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Обрабатываем результаты
            for (log_file, _, _), result in zip(files_to_download, results):
                if isinstance(result, Exception):
                    failed += 1
                else:
                    success, error_msg, file_size = result
                    if success:
                        downloaded += 1
                    else:
                        failed += 1
    
    # Завершаем прогресс-бар
    if total_files > 0:
        print()  # Переход на новую строку после прогресс-бара
    
    downloaded += existing_files_count
    
    return downloaded, failed


# Главная функция загрузки: находит и скачивает все логи за указанную дату со всех станций
def download_logs_for_date(target_date: str) -> None:
    """
    Скачивает логи для указанной даты со всех станций.
    
    Args:
        target_date: Дата в формате YYYYMMDD
    """
    try:
        stations, oper_stations, reg_stations, frames_stations, base_urls, headers = load_config()
    except FileNotFoundError as e:
        logger.error(f"Файл config.json не найден: {e}")
        print_error(f"Ошибка загрузки config.json: {e}", is_critical=True)
        return
    except (ValueError, RuntimeError) as e:
        logger.error(f"Ошибка загрузки config.json: {e}")
        print_error(f"Ошибка загрузки config.json: {e}", is_critical=True)
        return
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке конфигурации: {e}", exc_info=True)
        print_error(f"Неожиданная ошибка при загрузке конфигурации: {e}", is_critical=True, exc_info=True)
        return
    
    # Валидация и преобразование даты из YYYYMMDD в YYYY-MM-DD для параметров запроса
    try:
        date_obj = datetime.strptime(target_date, "%Y%m%d")
    except ValueError:
        print_error(f"Неверный формат даты '{target_date}'. Ожидается формат YYYYMMDD (например, 20251208)", is_critical=True)
        return
    
    # Запрашиваем диапазон ±1 день для гарантированного получения данных
    date_before = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
    date_after = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Загружаем страницы со списком логов (синхронно)
    all_logs = {}
    for st, st_list in [('oper', oper_stations), ('reg', reg_stations), ('frames', frames_stations)]:
        if not st_list:
            continue
        try:
            url = build_logs_url(st, base_urls, date_before, date_after)
            source_base_url = base_urls.get(st, base_urls.get('oper', 'https://eus.lorett.org/eus'))
            html = fetch_logs_page(url, st, headers)
            for station, station_logs in find_logs_in_page(html, target_date, st_list, source_base_url).items():
                all_logs.setdefault(station, []).extend(station_logs)
        except requests.RequestException as e:
            logger.warning(f"Ошибка сети при загрузке страницы для типа '{st}': {e}")
            print_error(f"Ошибка при загрузке страницы для типа '{st}': {e}")
        except (ValueError, KeyError) as e:
            logger.error(f"Ошибка данных при обработке типа '{st}': {e}")
            print_error(f"Ошибка данных при обработке типа '{st}': {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при обработке типа '{st}': {e}", exc_info=True)
            print_error(f"Неожиданная ошибка при обработке типа '{st}': {e}")
    
    total = sum(len(logs) for logs in all_logs.values())
    logger.info(f"Найдено {total} логов на {len(all_logs)} станциях для даты {target_date}")
    print(f"{Fore.CYAN + Style.BRIGHT}\nСТАТИСТИКА")
    print(f"{Fore.CYAN}Найдено: {total} логов на {len(all_logs)} станциях")
    for station, logs in sorted(all_logs.items()):
        logger.debug(f"Станция {station}: {len(logs)} логов")
        print(f"{Fore.CYAN}  {station}: {len(logs)}")
    
    if not all_logs:
        print(f"{Fore.YELLOW}Логи не найдены!")
        return
    
    # Создаем папку для логов в формате logs\YYYY\MM\YYYYMMDD
    year, month, date_str, _ = get_date_paths(target_date)
    logs_dir = Path('/root/lorett/GroundLinkMonitorServer/logs') / year / month / date_str
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"{Fore.CYAN + Style.BRIGHT}\nСКАЧИВАНИЕ")
    
    # Засекаем время начала загрузки
    start_time = time.time()
    
    # Используем асинхронную загрузку, если доступен aiohttp
    if AIOHTTP_AVAILABLE:
        try:
            downloaded, failed = asyncio.run(
                download_logs_async(all_logs, stations, base_urls, headers, logs_dir)
            )
        except RuntimeError:
            # Если event loop уже запущен, используем синхронную версию как fallback
            logger.warning("Event loop уже запущен, используется синхронная загрузка")
            downloaded, failed = _download_logs_sync(
                all_logs, stations, base_urls, headers, logs_dir, total
            )
    else:
        # Если aiohttp недоступен, используем синхронную загрузку
        logger.warning("aiohttp не установлен, используется синхронная загрузка")
        downloaded, failed = _download_logs_sync(
            all_logs, stations, base_urls, headers, logs_dir, total
        )
    
    # Засекаем время окончания загрузки
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Форматируем время
    if elapsed_time < 60:
        time_str = f"{elapsed_time:.2f} сек"
    elif elapsed_time < 3600:
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        time_str = f"{minutes} мин {seconds:.2f} сек"
    else:
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = elapsed_time % 60
        time_str = f"{hours} ч {minutes} мин {seconds:.2f} сек"
    
    logger.info(f"Загрузка завершена: скачано {downloaded}, ошибок {failed}, всего {total}, время {time_str}")
    print(f"{Fore.CYAN + Style.BRIGHT}\nРЕЗУЛЬТАТЫ")
    print(f"{Fore.GREEN if failed == 0 else Fore.RED}Скачано: {downloaded}, Ошибок: {failed}, Всего: {total}")
    print(f"{Fore.CYAN}Время загрузки: {time_str}")
    
    # Вычисляем среднюю скорость загрузки
    if elapsed_time > 0 and downloaded > 0:
        avg_speed = downloaded / elapsed_time
        logger.debug(f"Средняя скорость загрузки: {avg_speed:.2f} файлов/сек")
        print(f"{Fore.CYAN}Средняя скорость: {avg_speed:.2f} файлов/сек")


# Синхронная загрузка логов последовательно (fallback если aiohttp недоступен)
def _download_logs_sync(
    all_logs: Dict[str, List[Tuple[str, str]]],
    stations: Dict[str, str],
    base_urls: Dict[str, str],
    headers: Dict[str, str],
    logs_dir: Path,
    total: int
) -> Tuple[int, int]:
    """
    Синхронная загрузка логов.
    
    Args:
        all_logs: Словарь {имя_станции: [(имя_лог_файла, base_url), ...]}
        stations: Словарь {имя_станции: тип_станции}
        base_urls: Словарь базовых URL для типов станций (не используется, оставлен для совместимости)
        headers: HTTP заголовки
        logs_dir: Базовая директория для сохранения логов
        total: Общее количество логов
        
    Returns:
        Tuple[int, int]: (количество скачанных файлов, количество ошибок)
    """
    downloaded = failed = 0
    
    # Подготавливаем список всех файлов для загрузки
    all_files_to_download = []
    existing_files_count = 0
    
    for station_name, log_files_with_urls in sorted(all_logs.items()):
        # Проверка существования станции в конфиге
        if station_name not in stations:
            continue
        
        station_dir = logs_dir / station_name
        station_dir.mkdir(exist_ok=True)
        
        for log_file, source_base_url in sorted(log_files_with_urls):
            file_path = station_dir / log_file
            if file_path.exists():
                # Проверяем содержимое существующего файла
                if is_valid_log_file(file_path):
                    existing_files_count += 1
                    continue
                else:
                    # Файл содержит ошибку - удаляем его
                    file_path.unlink()
            
            # Используем base_url источника, на котором был найден лог
            all_files_to_download.append((log_file, file_path, source_base_url))
    
    total_to_download = len(all_files_to_download)
    total_files = total_to_download + existing_files_count
    
    # Выводим информацию о загрузке перед прогресс-баром
    if existing_files_count > 0 or total_to_download > 0:
        print(f"{Fore.CYAN}Загрузка: {existing_files_count} файлов уже существует, {total_to_download} файлов к загрузке")
    
    # Инициализируем прогресс-бар
    current_progress = existing_files_count
    if total_files > 0:
        update_progress_bar(current_progress, total_files)
    
    # Загружаем файлы с повторными попытками
    max_retries = 2
    for log_file, file_path, base_url in all_files_to_download:
        print(f"{Fore.CYAN}Загрузка: {log_file}")
        success = False
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                r = requests.get(urljoin(base_url, f"/eus/log_get/{log_file}"), 
                                headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
                r.raise_for_status()
                
                # Сохраняем содержимое во временный файл для проверки
                temp_file = file_path.with_suffix('.tmp')
                temp_file.write_bytes(r.content)
                
                # Проверяем содержимое файла на наличие ошибок
                is_valid, error_detail = validate_log_file_detailed(temp_file)
                if not is_valid:
                    # Файл содержит ошибку - не сохраняем его
                    try:
                        temp_file.unlink()
                    except (FileNotFoundError, PermissionError) as e:
                        logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")
                    last_error = error_detail if error_detail else "Лог содержит ошибки"
                    if attempt < max_retries:
                        print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                        time.sleep(1)  # Небольшая задержка перед повтором
                        continue
                    break
                
                # Файл валиден - переименовываем временный файл в постоянный
                try:
                    temp_file.replace(file_path)
                except (OSError, PermissionError) as e:
                    logger.error(f"Не удалось переместить файл {temp_file} в {file_path}: {e}")
                    last_error = f"Ошибка сохранения файла: {e}"
                    if attempt < max_retries:
                        print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                        time.sleep(1)
                        continue
                    break
                
                # Проверяем, что файл действительно загружен
                if is_log_file_downloaded(file_path):
                    success = True
                    downloaded += 1
                    print(f"{Fore.GREEN}  ✓ Загружен успешно")
                    break
                else:
                    last_error = "Файл не прошел проверку после загрузки"
                    if attempt < max_retries:
                        print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                        time.sleep(1)
                        continue
                    break
                    
            except requests.HTTPError as e:
                # Специальная обработка для 503 Service Unavailable
                if e.response is not None and e.response.status_code == 503:
                    delay = 2 ** attempt  # Экспоненциальная задержка: 2, 4, 8 секунд
                    last_error = f"Ошибка 503 (Service Unavailable)"
                    print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}. Повтор через {delay} сек...")
                    logger.warning(f"Ошибка 503 при загрузке {log_file}, попытка {attempt}/{max_retries}")
                    if attempt < max_retries:
                        time.sleep(delay)
                        continue
                    break
                else:
                    status = e.response.status_code if e.response is not None else "unknown"
                    last_error = f"Ошибка HTTP {status}: {e}"
                    if attempt < max_retries:
                        print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                        time.sleep(1)
                        continue
                    break
            except requests.RequestException as e:
                last_error = f"Ошибка сети: {e}"
                if attempt < max_retries:
                    print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                    time.sleep(1)
                    continue
                break
            except (OSError, PermissionError) as e:
                last_error = f"Ошибка файловой системы: {e}"
                if attempt < max_retries:
                    print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                    time.sleep(1)
                    continue
                break
            except Exception as e:
                last_error = f"Неожиданная ошибка: {e}"
                if attempt < max_retries:
                    print(f"{Fore.YELLOW}  Попытка {attempt}/{max_retries} не удалась: {last_error}")
                    time.sleep(1)
                    continue
                break
        
        if not success:
            failed += 1
            if last_error:
                print(f"{Fore.RED}  ✗ Не удалось загрузить после {max_retries} попыток: {last_error}")
                logger.error(f"Не удалось загрузить {log_file} после {max_retries} попыток: {last_error}")
        
        # Обновляем прогресс-бар
        current_progress += 1
        if total_files > 0:
            update_progress_bar(current_progress, total_files)
    
    # Завершаем прогресс-бар
    if total_files > 0:
        print()  # Переход на новую строку после прогресс-бара
    
    downloaded += existing_files_count
    
    return downloaded, failed


# Определяет тип диапазона (L или X) по количеству столбцов в заголовке лог-файла
def detect_bend_type_from_header(log_file_path: Path) -> str:
    """
    Определяет тип диапазона (L или X) из заголовка лог-файла.
    
    Args:
        log_file_path: Путь к лог-файлу
        
    Returns:
        str: "L" или "X"
    """
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#Time'):
                    # Анализируем заголовок
                    # L bend: #Time	Az	El	Level	SNR (5 столбцов до SNR)
                    # X bend: #Time	Az	El	Level	Level2	SNR (6 столбцов до SNR)
                    parts = line.split('\t')
                    if 'Level2' in line or len(parts) > 6:
                        return "X"
                    else:
                        return "L"
        # По умолчанию L, если не удалось определить
        return "L"
    except (FileNotFoundError, PermissionError, IOError, OSError) as e:
        logger.debug(f"Не удалось определить тип диапазона из файла {log_file_path}: {e}")
        return "L"


# Извлекает все значения SNR из лог-файла и возвращает сумму SNR и количество измерений
def extract_snr_from_log(log_file_path: Path, bend_type: Optional[str] = None) -> Tuple[float, int]:
    """
    Извлекает значения SNR из лог-файла и возвращает сумму SNR и количество измерений.
    Оптимизировано для минимизации потребления памяти.
    
    Args:
        log_file_path: Путь к лог-файлу
        bend_type: Тип диапазона ("L" или "X"). Если None, определяется автоматически из заголовка
        
    Returns:
        Tuple[float, int]: (сумма SNR, количество измерений)
    """
    snr_sum = 0.0
    snr_count = 0
    
    # Определяем тип диапазона, если не указан
    if bend_type is None:
        bend_type = detect_bend_type_from_header(log_file_path)
    
    # Определяем индекс столбца SNR в зависимости от типа диапазона
    # L bend: Time, Az, El, Level, SNR -> индекс 4
    # X bend: Time, Az, El, Level, Level2, SNR -> индекс 5
    snr_column_index = 5 if bend_type == "X" else 4
    
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Пропускаем комментарии и пустые строки без лишних операций
                if not line.strip() or line.startswith('#'):
                    continue
                
                # Разбиваем строку по табуляции
                parts = line.split('\t')
                
                if len(parts) > snr_column_index:
                    try:
                        snr_sum += float(parts[snr_column_index])
                        snr_count += 1
                    except (ValueError, IndexError):
                        continue
        
        return snr_sum, snr_count
            
    except (FileNotFoundError, PermissionError) as e:
        logger.error(f"Не удалось прочитать файл {log_file_path}: {e}")
        print_error(f"Ошибка доступа к файлу {log_file_path}: {e}", is_critical=False)
        return 0.0, 0
    except (IOError, OSError) as e:
        logger.error(f"Ошибка ввода-вывода при чтении файла {log_file_path}: {e}")
        print_error(f"Ошибка чтения файла {log_file_path}: {e}", is_critical=False)
        return 0.0, 0
    except Exception as e:
        logger.error(f"Неожиданная ошибка при чтении файла {log_file_path}: {e}", exc_info=True)
        print_error(f"Неожиданная ошибка при чтении файла {log_file_path}: {e}", is_critical=False)
        return 0.0, 0


# Рассчитывает статистику SNR для всех логов станции и выводит пустые пролеты
def calculate_avg_snr_for_station(station_folder: Path, bend_type: Optional[str] = None, show_only_unsuccessful: bool = True) -> List[Tuple[str, float, int]]:
    """
    Рассчитывает сумму SNR для всех лог-файлов в папке станции.
    
    Args:
        station_folder: Путь к папке станции
        bend_type: Тип диапазона ("L" или "X"). Если None, определяется автоматически
        show_only_unsuccessful: Если True, выводит только пустые пролеты
        
    Returns:
        List[Tuple[str, float, int]]: Список кортежей (имя файла, сумма SNR, количество измерений)
    """
    results = []
    
    if not station_folder.exists() or not station_folder.is_dir():
        print(f"{Fore.YELLOW}Папка {station_folder} не существует или не является директорией", file=sys.stderr)
        return results
    
    # Находим все .log файлы
    log_files = sorted(station_folder.glob("*.log"))
    
    if not log_files:
        print(f"{Fore.YELLOW}В папке {station_folder} не найдено лог-файлов", file=sys.stderr)
        return results
    
    print(f"\n{Fore.CYAN}Обработка станции: {station_folder.name}")
    print(f"{Fore.CYAN}Найдено лог-файлов: {len(log_files)}\n")
    
    # Определяем порог "пустоты" пролета в зависимости от типа диапазона
    bend_type_upper = (bend_type or "L").upper()
    threshold = X_BEND_FAILURE_THRESHOLD if bend_type_upper == "X" else L_BEND_FAILURE_THRESHOLD
    
    # Собираем все результаты
    for log_file in log_files:
        snr_sum, count = extract_snr_from_log(log_file, bend_type)
        results.append((log_file.name, snr_sum, count))
    
    # Фильтруем пустые пролеты, если требуется
    if show_only_unsuccessful:
        unsuccessful_results = []
        for filename, snr_sum, count in results:
            avg_snr = snr_sum / count if count > 0 else 0
            if avg_snr <= threshold:
                unsuccessful_results.append((filename, snr_sum, count))
        
        if unsuccessful_results:
            # Сортируем "пустые" результаты по сумме SNR (по возрастанию - от меньшего к большему)
            unsuccessful_results.sort(key=lambda x: x[1])
            
            # Выводим только пустые пролеты
            print(f"{Fore.RED}{'Пустые пролеты (средний SNR <= ' + str(threshold) + ')':<60}")
            print(f"{Fore.CYAN}{'Имя файла':<60} {'Сумма SNR':>12} {'Средний SNR':>15}")
            print("-" * 90)
            for filename, snr_sum, count in unsuccessful_results:
                avg_snr = snr_sum / count if count > 0 else 0
                print(f"{Fore.RED}{filename:<60} {snr_sum:>12.2f} {avg_snr:>15.2f}")
        else:
            print(f"{Fore.GREEN}Пустых пролетов не найдено")
    else:
        # Выводим все результаты (старое поведение)
        results.sort(key=lambda x: x[1], reverse=True)
        print(f"{Fore.CYAN}{'Имя файла':<60} {'Сумма SNR':>12}")
        print("-" * 75)
        for filename, snr_sum, count in results:
            print(f"{Fore.CYAN}{filename:<60} {snr_sum:>12.2f}")
    
    return results


# Получает график пролета через браузер (Playwright/Pyppeteer) и сохраняет как PNG
async def get_log_graph(station_name: str, log_filename: str, output_dir: Path):
    """
    Получает график пролета из веб-интерфейса и сохраняет как изображение.
    
    Args:
        station_name: Имя станции
        log_filename: Имя файла лога
        output_dir: Директория для сохранения изображения
    """
    # Подготавливаем путь к изображению
    output_dir.mkdir(parents=True, exist_ok=True)
    image_name = log_filename.replace('.log', '.png').replace(' ', '_')
    image_path = output_dir / image_name
    
    # Если изображение уже существует, пропускаем загрузку
    if image_path.exists():
        logger.debug(f"График уже существует, пропускаем: {image_path}")
        return image_path
    
    try:
        # Пробуем сначала playwright (более современный и надежный)
        try:
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                # Используем chromium с headless режимом
                browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
                page = await browser.new_page()
                url = f'http://eus.lorett.org/eus/log_view/{log_filename}'
                await page.goto(url, wait_until='networkidle', timeout=30000)
                await page.set_viewport_size({'width': GRAPH_VIEWPORT_WIDTH, 'height': GRAPH_VIEWPORT_HEIGHT})
                await asyncio.sleep(GRAPH_LOAD_DELAY)
                # Прокручиваем к начальной точке графика, если задано смещение
                if GRAPH_SCROLL_X > 0 or GRAPH_SCROLL_Y > 0:
                    await page.evaluate(f"window.scrollTo({GRAPH_SCROLL_X}, {GRAPH_SCROLL_Y})")
                    await asyncio.sleep(0.2)  # Небольшая задержка после скролла
                await page.screenshot(path=str(image_path), full_page=False)
                await browser.close()
            
            return image_path
        except ImportError:
            # Если playwright не установлен, пробуем pyppeteer
            from pyppeteer import launch
            import os
            import sys
            
            # Проверка уже выполнена в начале функции, но на всякий случай проверяем еще раз
            if image_path.exists():
                return image_path
            
            # Отключаем автоматическую загрузку Chromium
            os.environ['PYPPETEER_SKIP_CHROMIUM_DOWNLOAD'] = '1'
            
            # Пытаемся найти установленный Chrome/Chromium
            chrome_paths = [
                r'C:\Program Files\Google\Chrome\Application\chrome.exe',
                r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe',
                os.path.expanduser(r'~\AppData\Local\Google\Chrome\Application\chrome.exe'),
                r'C:\Program Files\Chromium\Application\chrome.exe',
            ]
            
            executable_path = None
            for path in chrome_paths:
                if os.path.exists(path):
                    executable_path = path
                    break
            
            if not executable_path:
                raise Exception("Chrome/Chromium не найден. Установите Chrome или используйте: pip install playwright && playwright install chromium")
            
            browser = await launch({
                'executablePath': executable_path,
                'args': ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
            })
            page = await browser.newPage()
            url = f'http://eus.lorett.org/eus/log_view/{log_filename}'
            await page.goto(url, waitUntil='networkidle0', timeout=30000)
            await page.setViewport({'width': GRAPH_VIEWPORT_WIDTH, 'height': GRAPH_VIEWPORT_HEIGHT})
            await asyncio.sleep(GRAPH_LOAD_DELAY)
            # Прокручиваем к начальной точке графика, если задано смещение
            if GRAPH_SCROLL_X > 0 or GRAPH_SCROLL_Y > 0:
                await page.evaluate(f"window.scrollTo({GRAPH_SCROLL_X}, {GRAPH_SCROLL_Y})")
                await asyncio.sleep(0.2)  # Небольшая задержка после скролла
            await page.screenshot({'path': str(image_path), 'fullPage': False})
            await browser.close()
            
            return image_path
    except ImportError:
        logger.warning("playwright или pyppeteer не установлены")
        print(f"{Fore.YELLOW}Предупреждение: playwright или pyppeteer не установлены. Установите: pip install playwright && playwright install chromium")
        return None
    except (asyncio.TimeoutError, RuntimeError) as e:
        logger.error(f"Ошибка при получении графика для {log_filename}: {e}")
        print_error(f"Ошибка при получении графика для {log_filename}: {e}", is_critical=False)
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении графика для {log_filename}: {e}", exc_info=True)
        print_error(f"Ошибка при получении графика для {log_filename}: {e}", is_critical=False)
        return None


# Главная функция анализа: обрабатывает логи, считает статистику SNR, загружает графики и генерирует отчет
def analyze_downloaded_logs(target_date: str) -> None:
    """
    Анализирует все скачанные лог-файлы за указанную дату.
    
    Рассчитывает сумму SNR для каждой станции, фильтрует пролеты по порогу SNR,
    определяет успешные/пустые пролеты и загружает графики для файлов
    с максимальной суммой SNR.
    
    Args:
        target_date: Дата в формате YYYYMMDD для анализа логов
    """
    # Используем папку для логов в формате logs\YYYY\MM\YYYYMMDD
    year, month, date_str, _ = get_date_paths(target_date)
    logs_dir = Path("/root/lorett/GroundLinkMonitorServer/logs")
    base_logs_dir = logs_dir / year / month / date_str
    
    if not base_logs_dir.exists():
        print(f"{Fore.YELLOW}Папка {base_logs_dir} не существует, анализ пропущен")
        return
    
    logger.info(f"Начало анализа SNR для даты {target_date}")
    print(f"{Fore.CYAN + Style.BRIGHT}\nАНАЛИЗ SNR")
    print(f"{Fore.BLUE}Обработка логов за дату: {target_date}")
    
    # Загружаем конфигурацию для получения типов диапазонов
    try:
        stations, station_bend_map = load_stations_from_config_for_analysis()
        config_path = Path('/root/lorett/GroundLinkMonitorServer/config.json')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        else:
            config = {}
    except SystemExit:
        stations = {}
        station_bend_map = {}
        config = {}
        print(f"{Fore.YELLOW}Предупреждение: не удалось загрузить конфигурацию, анализ будет без учета типов диапазонов")
    
    all_results = {}
    
    # Обрабатываем все станции из конфига
    for station_name in stations.keys():
        station_folder = base_logs_dir / station_name
        
        bend_type = station_bend_map.get(station_name)
        bend_type_upper = (bend_type or "L").upper()
        
        # Инициализируем запись для всех станций из конфига
        all_results[station_name] = {
            'files': 0,
            'avg_snr': 0.0,
            'measurements': 0,
            'max_snr_filename': '',
            'max_snr_value': 0.0,
            'successful_passes': 0,
            'unsuccessful_passes': 0,
            'unsuccessful_filenames': [],  # Список пустых пролетов
            'bend_type': bend_type_upper,
            'results': []
        }
        
        if not station_folder.exists():
            continue
        
        if bend_type:
            print(f"\n{Fore.CYAN}Обработка станции {station_name} (диапазон: {bend_type})")
        else:
            print(f"\n{Fore.CYAN}Обработка станции {station_name}")
        
        # Выводим только пустые пролеты
        results = calculate_avg_snr_for_station(station_folder, bend_type, show_only_unsuccessful=True)
        
        if results:
            # Для станции R2.0S_Moscow исключаем пролеты TY из подсчета
            if station_name == "R2.0S_Moscow":
                original_count = len(results)
                results = [(filename, snr_sum, count) for filename, snr_sum, count in results if "TY" not in filename]
                filtered_count = original_count - len(results)
                if filtered_count > 0:
                    logger.info(f"Станция {station_name}: исключено {filtered_count} пролетов TY из подсчета")
                if not results:
                    logger.info(f"Станция {station_name}: все пролеты были отфильтрованы (TY пролеты исключены)")
                    continue
            
            # Фильтруем пролеты: оставляем только те, где средний SNR > MIN_AVG_SNR_THRESHOLD
            filtered_results = []
            for filename, snr_sum, count in results:
                avg_snr = snr_sum / count if count > 0 else 0
                if avg_snr > MIN_AVG_SNR_THRESHOLD:
                    filtered_results.append((filename, snr_sum, count))
            
            # Вычисляем общую статистику
            total_files = len(results)
            
            # Для расчета суммы SNR используем только отфильтрованные результаты (средний SNR > 4)
            if filtered_results:
                # Сортируем отфильтрованные результаты по сумме SNR (по убыванию)
                filtered_results.sort(key=lambda x: x[1], reverse=True)
                # Вычисляем средний SNR для станции из отфильтрованных результатов
                total_sum = sum(snr_sum for _, snr_sum, _ in filtered_results)
                total_measurements = sum(count for _, _, count in filtered_results)
                total_avg = total_sum / total_measurements if total_measurements > 0 else 0
                # Первый в отсортированном списке - с максимальной суммой SNR
                max_snr_filename, max_snr_value, _ = filtered_results[0]
            else:
                # Если нет пролетов со средним SNR > 4, используем все результаты
                total_sum = sum(snr_sum for _, snr_sum, _ in results)
                total_measurements = sum(count for _, _, count in results)
                total_avg = total_sum / total_measurements if total_measurements > 0 else 0
                # Результаты уже отсортированы по сумме SNR в порядке убывания
                max_snr_filename, max_snr_value, _ = results[0]
            
            # Подсчет успешных и пустых пролетов
            successful_passes = 0
            unsuccessful_passes = 0
            unsuccessful_filenames = []  # Список имен пустых пролетов
            successful_results = []  # (filename, snr_sum, count) только успешные пролеты
            
            # Определяем порог успешности в зависимости от диапазона
            threshold = X_BEND_FAILURE_THRESHOLD if bend_type_upper == "X" else L_BEND_FAILURE_THRESHOLD
            
            for filename, snr_sum, count in results:
                # Вычисляем среднее SNR для определения успешности пролета
                avg_snr = snr_sum / count if count > 0 else 0
                if avg_snr > threshold:
                    successful_passes += 1
                    successful_results.append((filename, snr_sum, count))
                else:
                    unsuccessful_passes += 1
                    unsuccessful_filenames.append(filename)

            # Если есть хотя бы один успешный пролет — выбираем "макс. сумму SNR" среди успешных,
            # чтобы график строился по реально успешному лог-файлу.
            if successful_results:
                successful_results.sort(key=lambda x: x[1], reverse=True)
                max_snr_filename, max_snr_value, _ = successful_results[0]
            
            all_results[station_name] = {
                'files': total_files,
                'avg_snr': total_avg,
                'measurements': total_measurements,
                'max_snr_filename': max_snr_filename,
                'max_snr_value': max_snr_value,
                'successful_passes': successful_passes,
                'unsuccessful_passes': unsuccessful_passes,
                'unsuccessful_filenames': unsuccessful_filenames,  # Список пустых пролетов
                'bend_type': bend_type_upper,
                'results': results
            }
            
            logger.info(f"Станция {station_name}: файлов {total_files}, средний SNR {total_avg:.2f}, успешных {successful_passes}, пустых {unsuccessful_passes}")
            print(f"{Fore.GREEN}  Всего файлов: {total_files}, Средний SNR: {total_avg:.2f}")
            print(f"{Fore.GREEN}  Успешных пролетов: {successful_passes}, Пустых пролетов: {unsuccessful_passes}")
            
            # Сохраняем результаты в файл
            output_file = station_folder / f"avg_snr_{station_folder.name}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"Сумма SNR для станции: {station_folder.name}\n")
                f.write("\n")
                f.write(f"{'Имя файла':<60} {'Сумма SNR':>12}\n")
                f.write("-" * 75 + "\n")
                for filename, snr_sum, count in results:
                    f.write(f"{filename:<60} {snr_sum:>12.2f}\n")
                f.write("\n")
                f.write(f"Всего файлов обработано: {total_files}\n")
                f.write(f"Средний SNR по всем измерениям: {total_avg:.2f}\n")
                f.write(f"Успешных пролетов: {successful_passes}\n")
                f.write(f"Пустых пролетов: {unsuccessful_passes}\n")
            
            print(f"{Fore.GREEN}  Результаты сохранены в файл: {output_file}")
    
    # Итоговая сводка
    if stations:
        # Преобразуем дату из YYYYMMDD в YYYY-MM-DD для отображения
        date_display = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
        print(f"\n{Fore.CYAN + Style.BRIGHT}\nИТОГОВАЯ СВОДКА ПО ВСЕМ СТАНЦИЯМ  {date_display}")
        print(f"{Fore.CYAN}{'Станция':<30} {'Всего':>10} {'Успешных':>12} {'Пустых':>14} {'% пустых':>15} {'Средний SNR':>15}")
        print("-" * 115)
        
        # Сортируем станции по среднему SNR
        sorted_stations = sorted(all_results.items(), key=lambda x: x[1]['avg_snr'], reverse=True)
        
        for station_name, stats in sorted_stations:
            unsuccessful_percent = (stats['unsuccessful_passes'] / stats['files'] * 100) if stats['files'] > 0 else 0.0
            print(f"{Fore.CYAN}{station_name:<30} {stats['files']:>10} {stats['successful_passes']:>12} {stats['unsuccessful_passes']:>14} {unsuccessful_percent:>14.1f}% {stats['avg_snr']:>15.2f}")
        
        total_all_files = sum(stats['files'] for stats in all_results.values())
        total_successful = sum(stats['successful_passes'] for stats in all_results.values())
        total_unsuccessful = sum(stats['unsuccessful_passes'] for stats in all_results.values())
        total_unsuccessful_percent = (total_unsuccessful / total_all_files * 100) if total_all_files > 0 else 0.0
        # Вычисляем общий средний SNR: сумма всех SNR / количество всех измерений
        total_all_measurements = sum(stats['measurements'] for stats in all_results.values())
        total_all_snr_sum = sum(stats['avg_snr'] * stats['measurements'] for stats in all_results.values())
        overall_avg = total_all_snr_sum / total_all_measurements if total_all_measurements > 0 else 0
        
        print("-" * 115)
        print(f"{Fore.GREEN + Style.BRIGHT}{'ИТОГО':<30} {total_all_files:>10} {total_successful:>12} {total_unsuccessful:>14} {total_unsuccessful_percent:>14.1f}% {overall_avg:>15.2f}")
        
        # Список файлов с максимальной суммой SNR
        print(f"\n{Fore.CYAN + Style.BRIGHT}\nФАЙЛЫ С МАКСИМАЛЬНОЙ СУММОЙ SNR ПО СТАНЦИЯМ")
        print(f"{Fore.CYAN}{'Станция':<30} {'Файл с макс. суммой SNR':<80} {'Сумма SNR':>15}")
        print("-" * 140)
        
        # Создаем папку для графиков в формате report\YYYY\MM\DD.MM.YYYY
        # ВАЖНО: не удаляем существующую директорию, чтобы не скачивать уже сохраненные графики повторно.
        year, month, _, date_folder = get_date_paths(target_date)
        graphs_dir = Path('/root/lorett/GroundLinkMonitorServer/report') / year / month / date_folder
        try:
            graphs_dir.mkdir(parents=True, exist_ok=True)
        except (PermissionError, OSError) as e:
            logger.error(f"Не удалось создать директорию {graphs_dir}: {e}")
            print_error(f"Не удалось создать директорию для графиков: {e}", is_critical=True)
            return
        
        # Собираем задачи для получения графиков
        graph_tasks = []
        
        for station_name, stats in sorted_stations:
            if stats['files'] == 0:
                print(f"{Fore.YELLOW}{station_name:<30} {'нет данных':<80} {'':>15}")
            else:
                # Станцию считаем "не работает" только если нет ни одного успешного пролета
                # (avg_snr по станции может быть низким из-за большого количества пустых пролетов).
                if stats.get('successful_passes', 0) <= 0 or not stats.get('max_snr_filename'):
                    print(f"{Fore.RED}{station_name:<30} {'станция не работает':<80} {'':>15}")
                else:
                    print(f"{Fore.CYAN}{station_name:<30} {stats['max_snr_filename']:<80} {stats['max_snr_value']:>15.2f}")
                    # Добавляем задачу для получения графика
                    graph_tasks.append((station_name, stats['max_snr_filename']))
        
        # Получаем графики для файлов с максимальной суммой SNR
        if graph_tasks:
            print(f"\n{Fore.CYAN + Style.BRIGHT}\nЗАГРУЗКА ГРАФИКОВ ПРОЛЕТОВ С МАКСИМАЛЬНОЙ СУММОЙ SNR")
            async def download_all_graphs():
                tasks = []
                for station_name, log_filename in graph_tasks:
                    print(f"{Fore.BLUE}Загрузка графика: {station_name} - {log_filename}")
                    task = get_log_graph(station_name, log_filename, graphs_dir)
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, (station_name, log_filename) in enumerate(graph_tasks):
                    if isinstance(results[i], Exception):
                        print(f"{Fore.RED}Ошибка при загрузке графика для {log_filename}: {results[i]}")
                    elif results[i]:
                        print(f"{Fore.GREEN}График сохранен: {results[i]}")
            
            try:
                # Исправление проблемы с asyncio.get_event_loop()
                # Проверяем, есть ли уже запущенный event loop
                try:
                    # Пытаемся получить текущий running loop
                    asyncio.get_running_loop()
                    # Если дошли сюда, значит loop уже запущен - используем create_task
                    # Но так как мы в синхронной функции, лучше создать новый loop
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(download_all_graphs())
                    loop.close()
                except RuntimeError:
                    # Если loop не запущен, используем asyncio.run() (Python 3.7+)
                    try:
                        asyncio.run(download_all_graphs())
                    except AttributeError:
                        # Для старых версий Python (< 3.7) используем get_event_loop()
                        loop = asyncio.get_event_loop()
                        if loop.is_closed():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                        loop.run_until_complete(download_all_graphs())
            except (RuntimeError, asyncio.TimeoutError) as e:
                logger.error(f"Ошибка при загрузке графиков: {e}", exc_info=True)
                print_error(f"Ошибка при загрузке графиков: {e}", is_critical=False)
            except Exception as e:
                logger.error(f"Неожиданная ошибка при загрузке графиков: {e}", exc_info=True)
                print_error(f"Неожиданная ошибка при загрузке графиков: {e}", is_critical=False)
        
        # Отправляем статистику на почту (настройки по умолчанию берём из test_email.py,
        # но можно переопределить через config.json -> email.* или переменные окружения)
        try:
            email_settings = get_email_settings(config)
            if email_settings.get("enabled"):
                subject = "Сводка работы станций"
                # Генерируем сводный график общего % пустых за 7 дней
                summary_chart_path = graphs_dir / "overall_unsuccessful_7d.png"
                generated_summary = generate_overall_unsuccessful_7d_chart(
                    target_date=target_date,
                    stations=stations,
                    station_bend_map=station_bend_map,
                    output_path=summary_chart_path,
                    days=7,
                )

                body, inline_images = build_stats_email_body(
                    target_date,
                    all_results,
                    graphs_dir,
                    generated_summary,
                )
                attachments = []

                ok = send_stats_email(
                    smtp_server=email_settings["smtp_server"],
                    smtp_port=int(email_settings["smtp_port"]),
                    sender_email=email_settings["sender_email"],
                    sender_password=email_settings["sender_password"],
                    recipients=email_settings["recipients"],
                    cc_recipients=email_settings.get("cc_recipients") or [],
                    subject=subject,
                    body=body,
                    attachments=attachments,
                    inline_images=inline_images,
                )
                if ok:
                    print(f"{Fore.GREEN}Статистика отправлена на почту: {', '.join(email_settings['recipients'])}")
                else:
                    print(f"{Fore.YELLOW}Предупреждение: не удалось отправить статистику на почту (см. лог)")
        except Exception as e:
            logger.warning(f"Неожиданная ошибка при отправке email: {e}", exc_info=True)
            print(f"{Fore.YELLOW}Предупреждение: не удалось отправить email: {e}")
        

# Загружает конфигурацию станций и создает словари: станция->тип и станция->диапазон (bend)
def load_stations_from_config_for_analysis(config_path: Path = Path("/root/lorett/GroundLinkMonitorServer/config.json")) -> Tuple[dict, dict]:
    """
    Загружает список станций из config.json и создает словарь соответствия станция -> bend тип.
    Используется для анализа SNR.
    
    Args:
        config_path: Путь к файлу конфигурации
        
    Returns:
        Tuple[dict, dict]: (словарь {имя_станции: тип_станции}, словарь {имя_станции: bend_тип})
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            stations_dict = {}
            station_bend_map = {}
            
            for station in config.get('stations', []):
                station_name = station['name']
                stations_dict[station_name] = station.get('type', 'oper')
                # Используем "bend" или "range" (для обратной совместимости)
                bend_type = station.get('bend') or station.get('range')
                if bend_type:
                    station_bend_map[station_name] = bend_type.upper()
            
            return stations_dict, station_bend_map
    except FileNotFoundError:
        print(f"{Fore.RED}Ошибка: файл конфигурации {config_path} не найден", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"{Fore.RED}Ошибка при чтении конфигурации: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyError as e:
        print(f"{Fore.RED}Ошибка в структуре конфигурации: {e}", file=sys.stderr)
        sys.exit(1)


def run_daily_report():
    """
    Запускает обработку и отправку статистики за вчерашний день.
    Вызывается автоматически в 00:00 UTC.
    """
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("НАЧАЛО АВТОМАТИЧЕСКОЙ ОТПРАВКИ СТАТИСТИКИ")
    logger.info("=" * 60)
    
    try:
        # Получаем вчерашнюю дату в UTC
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime("%Y%m%d")
        date_display = yesterday.strftime("%d.%m.%Y")
        
        logger.info(f"Дата для отчёта: {date_display} ({date_str})")
        logger.info(f"Время запуска: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"{Fore.CYAN + Style.BRIGHT}\n{'='*80}")
        print(f"{Fore.CYAN + Style.BRIGHT}АВТОМАТИЧЕСКАЯ ОТПРАВКА СТАТИСТИКИ")
        print(f"{Fore.CYAN}Дата: {date_display} ({date_str})")
        print(f"{Fore.CYAN + Style.BRIGHT}{'='*80}\n")
        
        # Сначала скачиваем логи
        logger.info("Этап 1/2: Скачивание логов...")
        download_start = datetime.now(timezone.utc)
        download_logs_for_date(date_str)
        download_duration = (datetime.now(timezone.utc) - download_start).total_seconds()
        logger.info(f"Этап 1/2: Скачивание логов завершено за {download_duration:.1f} сек")
        
        # Затем анализируем и отправляем статистику
        logger.info("Этап 2/2: Анализ логов и отправка email...")
        analyze_start = datetime.now(timezone.utc)
        analyze_downloaded_logs(date_str)
        analyze_duration = (datetime.now(timezone.utc) - analyze_start).total_seconds()
        logger.info(f"Этап 2/2: Анализ и отправка завершены за {analyze_duration:.1f} сек")
        
        total_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"Автоматическая отправка статистики за {date_str} завершена успешно")
        logger.info(f"Общее время выполнения: {total_duration:.1f} сек")
        logger.info("=" * 60)
        print(f"{Fore.GREEN}Автоматическая отправка статистики за {date_display} завершена")
    except Exception as e:
        total_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.error(f"ОШИБКА при автоматической отправке статистики: {e}")
        logger.error(f"Время до ошибки: {total_duration:.1f} сек")
        logger.error("Traceback:", exc_info=True)
        logger.info("=" * 60)
        print(f"{Fore.RED}Ошибка при автоматической отправке статистики: {e}")


def scheduler_loop():
    """
    Планировщик, который запускает отправку статистики в 00:00 UTC каждый день.
    """
    logger.info("=" * 60)
    logger.info("ПЛАНИРОВЩИК LORETT GROUND LINK MONITOR ЗАПУЩЕН")
    logger.info("=" * 60)
    logger.info(f"Время запуска: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("Режим: ежедневная отправка статистики в 00:00 UTC")
    print(f"{Fore.GREEN}Планировщик автоматической отправки статистики запущен")
    print(f"{Fore.CYAN}Отправка будет происходить каждый день в 00:00 UTC")
    print(f"{Fore.CYAN}Для остановки нажмите Ctrl+C\n")
    
    report_count = 0
    error_count = 0
    
    while True:
        try:
            # Получаем текущее время в UTC
            now_utc = datetime.now(timezone.utc)
            
            # Вычисляем время до следующей полуночи UTC
            next_midnight_utc = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            if now_utc.hour == 0 and now_utc.minute == 0:
                # Если уже 00:00, запускаем сразу
                next_midnight_utc = now_utc.replace(second=0, microsecond=0)
            
            wait_seconds = (next_midnight_utc - now_utc).total_seconds()
            
            # Если до полуночи меньше минуты, запускаем сразу
            if wait_seconds < 60:
                logger.info("До полуночи UTC меньше минуты, запускаем отправку немедленно")
                report_count += 1
                logger.info(f"Отчёт #{report_count} начинается...")
                run_daily_report()
                logger.info(f"Отчёт #{report_count} завершён")
                # После отправки ждём до следующей полуночи
                next_midnight_utc = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                wait_seconds = (next_midnight_utc - datetime.now(timezone.utc)).total_seconds()
            
            wait_hours = wait_seconds / 3600
            wait_minutes = (wait_seconds % 3600) / 60
            next_time_str = next_midnight_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
            
            logger.info(f"Ожидание следующей отправки: {next_time_str}")
            logger.info(f"Время ожидания: {int(wait_hours)} ч {int(wait_minutes)} мин")
            logger.info(f"Статистика: отчётов отправлено: {report_count}, ошибок: {error_count}")
            print(f"{Fore.CYAN}Следующая отправка: {next_time_str} (через {wait_hours:.1f} часов)")
            
            # Ждём до следующей полуночи
            time.sleep(wait_seconds)
            
            # Запускаем отправку
            logger.info("Время отправки наступило!")
            report_count += 1
            logger.info(f"Отчёт #{report_count} начинается...")
            run_daily_report()
            logger.info(f"Отчёт #{report_count} завершён")
            
        except KeyboardInterrupt:
            logger.info("=" * 60)
            logger.info("ПЛАНИРОВЩИК ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)")
            logger.info(f"Итого отправлено отчётов: {report_count}")
            logger.info(f"Итого ошибок: {error_count}")
            logger.info("=" * 60)
            print(f"\n{Fore.YELLOW}Планировщик остановлен")
            break
        except Exception as e:
            error_count += 1
            logger.error(f"ОШИБКА #{error_count} в планировщике: {e}")
            logger.error("Traceback:", exc_info=True)
            logger.info(f"Повторная попытка через 60 секунд...")
            print(f"{Fore.RED}Ошибка в планировщике: {e}")
            # Ждём минуту перед повтором
            time.sleep(60)


if __name__ == "__main__":
    import sys
    # Проверяем, запущен ли скрипт в режиме планировщика
    if len(sys.argv) >= 2 and sys.argv[1] == "--scheduler":
        scheduler_loop()
        sys.exit(0)
    
    if len(sys.argv) < 2:
        # Если дата не указана, используем текущую дату в UTC
        start_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        print(f"{Fore.CYAN}Дата не указана, используется текущая дата (UTC): {start_date_str}")
    else:
        start_date_str = sys.argv[1]
    
    # Проверяем валидность первой даты
    try:
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
    except ValueError:
        print(f"{Fore.RED + Style.BRIGHT}Ошибка: неверный формат даты '{start_date_str}'. Ожидается формат YYYYMMDD")
        sys.exit(1)
    
    # Проверяем, указан ли диапазон дат
    if len(sys.argv) >= 3:
        end_date_str = sys.argv[2]
        try:
            end_date = datetime.strptime(end_date_str, "%Y%m%d")
        except ValueError:
            print(f"{Fore.RED + Style.BRIGHT}Ошибка: неверный формат даты '{end_date_str}'. Ожидается формат YYYYMMDD")
            sys.exit(1)
        
        # Проверяем, что конечная дата не раньше начальной
        if end_date < start_date:
            print(f"{Fore.RED + Style.BRIGHT}Ошибка: конечная дата не может быть раньше начальной")
            sys.exit(1)
        
        # Обрабатываем диапазон дат
        print(f"{Fore.CYAN + Style.BRIGHT}\nОБРАБОТКА ДИАПАЗОНА ДАТ")
        print(f"{Fore.CYAN}С {start_date_str} по {end_date_str}")
        
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        day_num = 0
        
        while current_date <= end_date:
            day_num += 1
            date_str = current_date.strftime("%Y%m%d")
            date_display = current_date.strftime("%d.%m.%Y")
            
            print(f"\n{Fore.CYAN + Style.BRIGHT}{'='*80}")
            print(f"{Fore.CYAN + Style.BRIGHT}ДЕНЬ {day_num} из {total_days}: {date_display} ({date_str})")
            print(f"{Fore.CYAN + Style.BRIGHT}{'='*80}\n")
            
            try:
    # Сначала скачиваем логи
                download_logs_for_date(date_str)
    
    # Затем анализируем скачанные логи
                analyze_downloaded_logs(date_str)
            except (FileNotFoundError, PermissionError) as e:
                logger.error(f"Ошибка доступа при обработке даты {date_str}: {e}")
                print_error(f"Ошибка доступа при обработке даты {date_str}: {e}", is_critical=False)
                print(f"{Fore.YELLOW}Продолжаем обработку следующей даты...")
            except (ValueError, KeyError) as e:
                logger.error(f"Ошибка данных при обработке даты {date_str}: {e}")
                print_error(f"Ошибка данных при обработке даты {date_str}: {e}", is_critical=False)
                print(f"{Fore.YELLOW}Продолжаем обработку следующей даты...")
            except Exception as e:
                logger.error(f"Неожиданная ошибка при обработке даты {date_str}: {e}", exc_info=True)
                print_error(f"Ошибка при обработке даты {date_str}: {e}", is_critical=False)
                print(f"{Fore.YELLOW}Продолжаем обработку следующей даты...")
            
            # Переходим к следующему дню
            current_date += timedelta(days=1)
        
        print(f"\n{Fore.GREEN + Style.BRIGHT}\nОБРАБОТКА ДИАПАЗОНА ДАТ ЗАВЕРШЕНА")
        print(f"{Fore.GREEN}Обработано дней: {total_days}")
    else:
        # Обрабатываем одну дату
        date_str = start_date_str
        
        # Сначала скачиваем логи
        download_logs_for_date(date_str)
        
        # Затем анализируем скачанные логи
        analyze_downloaded_logs(date_str)
