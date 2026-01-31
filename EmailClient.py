"""EmailClient — отправка писем со статистикой по станциям приёма.

Модуль формирует HTML-письма со сводкой по пролётам и коммерческим сессиям,
поддерживает inline-графики и вложения. Настройки берутся из config.json
(секция "email"), переменных окружения и (fallback) test_email.py.

Структура config["email"]:
    enabled, smtp_server, smtp_port, sender_email, sender_password,
    recipient_email (или to), cc (или cc_emails), subject, attach_report.

Пример использования:
    client = EmailClient(logger=logger, config=config)
    settings = client.get_email_settings()
    if settings["enabled"]:
        body, inline_images = client.build_stats_email_body(target_date, all_results, ...)
        client.send_stats_email(..., body=body, inline_images=inline_images)
"""

import os
import re
import smtplib
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from typing import Any, Dict, List, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from Logger import Logger


class EmailClient:
    """Клиент для отправки писем со статистикой по станциям приёма.

    Отвечает за:
        - Сбор настроек SMTP из config, env и test_email.py
        - Формирование HTML-шаблона письма (таблицы, графики, списки пролётов)
        - Отправку через SMTP с поддержкой inline-изображений и вложений

    Методы:
        __init__: Инициализация клиента с логгером и конфигурацией.
        _load_email_defaults_from_test_email: Загрузка SMTP-настроек из test_email.py.
        get_email_settings: Получение настроек email из config, env и test_email.
        build_stats_email_body: Формирование HTML-тела письма со статистикой и графиками.
        send_stats_email: Отправка письма через SMTP с inline-изображениями и вложениями.

    Атрибуты:
        logger: Логгер для сообщений.
        config: Конфигурация (dict), используется при get_email_settings если не передан явно.
    """

    # Инициализация клиента с логгером и конфигурацией.
    def __init__(self, logger: Logger, config: Optional[Dict[str, Any]] = None) -> None:
        """Инициализирует клиент.

        Args:
            logger: Логгер (обязателен).
            config: Конфигурация приложения. Секция email используется для настроек SMTP.

        Raises:
            ValueError: Если logger не передан.
        """
        if logger is None:
            raise ValueError("logger is required")
        self.logger = logger
        self.config = (config or {}) if isinstance(config, dict) else {}

    # Загрузка SMTP-настроек по умолчанию из test_email.py.
    def _load_email_defaults_from_test_email(self) -> Dict[str, Any]:
        """Загружает SMTP-настройки по умолчанию из test_email.py (если модуль доступен).

        Returns:
            dict: Ключи smtp_server, smtp_port, sender_email, sender_password,
                  recipient_email, subject. Пустой dict при ошибке.
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

    # Возвращает настройки email из config, env и test_email.
    def get_email_settings(
        self,
        config: Optional[Dict[str, Any]] = None,
        debug_recipient: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Возвращает настройки для отправки email.

        Источники (в порядке приоритета): config["email"], переменные окружения,
        test_email.py. Переменные окружения: EMAIL_ENABLED, SMTP_SERVER, SMTP_PORT,
        SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAIL, EMAIL_CC, EMAIL_SUBJECT,
        EMAIL_ATTACH_REPORT.

        Args:
            config: Конфигурация (если None — используется self.config).
            debug_recipient: Адрес для отладочной отправки (все письма только на него, CC сбрасывается).

        Returns:
            dict с ключами: enabled, smtp_server, smtp_port, sender_email,
            sender_password, recipients, cc_recipients, subject, attach_report.
        """
        defaults = self._load_email_defaults_from_test_email()
        cfg = config if config is not None else self.config
        email_cfg = (cfg or {}).get("email", {}) if isinstance(cfg, dict) else {}

        enabled_raw = email_cfg.get("enabled", os.getenv("EMAIL_ENABLED"))
        if enabled_raw is None:
            enabled = True
        elif isinstance(enabled_raw, bool):
            enabled = enabled_raw
        else:
            enabled = str(enabled_raw).strip().lower() in ("1", "true", "yes", "y", "on")

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

        recipient_raw = (
            email_cfg.get("recipient_email")
            or email_cfg.get("to")
            or os.getenv("RECIPIENT_EMAIL")
            or defaults.get("recipient_email")
            or ""
        )
        recipients = [r.strip() for r in re.split(r"[;,]", str(recipient_raw)) if r.strip()]

        cc_raw = (
            email_cfg.get("cc")
            or email_cfg.get("cc_emails")
            or email_cfg.get("сс")
            or email_cfg.get("СС")
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

        recipients_final = recipients
        cc_final = cc_recipients
        if debug_recipient:
            recipients_final = [debug_recipient]
            cc_final = []

        return {
            "enabled": enabled,
            "smtp_server": smtp_server,
            "smtp_port": smtp_port,
            "sender_email": sender_email,
            "sender_password": sender_password,
            "recipients": recipients_final,
            "cc_recipients": cc_final,
            "subject": subject,
            "attach_report": attach_report,
        }

    # Формирует HTML-тело письма со статистикой и графиками.
    def build_stats_email_body(
        self,
        target_date: str,
        all_results: Dict[str, Dict[str, Any]],
        graphs_dir: Optional[Path] = None,
        summary_7d_chart_path: Optional[Path] = None,
        comm_stats: Optional[Dict[str, Dict[str, int]]] = None,
        comm_totals: Optional[Dict[str, int]] = None,
        comm_summary_7d_chart_path: Optional[Path] = None,
        comm_links: Optional[Dict[str, List[str]]] = None,
        comm_not_received_list: Optional[List[Tuple[str, str, str, str, str]]] = None,
        ) -> Tuple[str, Dict[str, Path]]:
        """Формирует HTML-тело письма со статистикой и графиками.

        Структура письма: блок коммерческих пролётов (если передан), общая статистика
        по станциям, сводный график за 7 дней, графики по станциям (лучший пролёт,
        % пустых за 7 дней, список пустых).

        Args:
            target_date: Дата в формате YYYYMMDD.
            all_results: {station_name: {files, successful_passes, unsuccessful_passes,
                        avg_snr, max_snr_filename, unsuccessful_filenames, best_graph_path,
                        station_7d_chart_path}}.
            graphs_dir: Каталог с графиками для поиска best_graph по имени файла.
            summary_7d_chart_path: Путь к сводному графику % пустых за 7 дней.
            comm_stats: {station: {planned, successful, not_received}} для коммерческих.
            comm_totals: {planned, successful, not_received} — итоги коммерческих.
            comm_summary_7d_chart_path: График % непринятых коммерческих за 7 дней.
            comm_links: {successful: [urls], unsuccessful: [urls]} — ссылки на графики.
            comm_not_received_list: [(station, satellite, rx_start, rx_end, graph_url), ...].

        Returns:
            (html_body: str, inline_images: {cid: Path}) — тело письма и карта CID→файл.
        """
        date_display = f"{target_date[6:8]}.{target_date[4:6]}.{target_date[0:4]}"
        inline_images: Dict[str, Path] = {}

        # Данные как в old_GroundLinkServer: files, successful_passes, unsuccessful_passes (с fallback для total_files)
        def _files(s: Dict[str, Any]) -> int:
            return int(s.get("files", s.get("total_files", 0)) or 0)

        def _successful(s: Dict[str, Any]) -> int:
            v = s.get("successful_passes")
            if v is not None:
                return int(v) if v else 0
            return _files(s) - _unsuccessful(s)

        def _unsuccessful(s: Dict[str, Any]) -> int:
            return int(s.get("unsuccessful_passes", 0) or 0)

        # Сортируем станции по возрастанию процента пустых пролетов
        def _unsuccessful_pct(s: Dict[str, Any]) -> float:
            f = _files(s)
            if f <= 0:
                return 100.0
            return (_unsuccessful(s) / f) * 100.0

        sorted_stations = sorted(
            all_results.items(),
            key=lambda x: _unsuccessful_pct(x[1]),
            reverse=False,
        )

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
            "    font-size: 14px;",
            "    line-height: 1.6;",
            "    color: #1d1d1f;",
            "    background-color: #f5f5f7;",
            "    margin: 0;",
            "    padding: 10px 6px;",
            "    -webkit-text-size-adjust: 100%;",
            "    -ms-text-size-adjust: 100%;",
            "  }",
            "  .container {",
            "    max-width: 820px;",
            "    margin: 0 auto;",
            "    background-color: #ffffff;",
            "    border-radius: 12px;",
            "    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);",
            "    overflow: hidden;",
            "  }",
            "  .header {",
            "    padding: 16px 14px 12px;",
            "    border-bottom: 1px solid #e5e5e7;",
            "    background: linear-gradient(to bottom, #ffffff, #fafafa);",
            "  }",
            "  h2 {",
            "    font-size: 18px;",
            "    font-weight: 600;",
            "    letter-spacing: -0.5px;",
            "    color: #1d1d1f;",
            "    margin: 0 0 8px 0;",
            "  }",
            "  .date {",
            "    font-size: 15px;",
            "    color: #86868b;",
            "    font-weight: 400;",
            "    margin: 0;",
            "  }",
            "  .content {",
            "    padding: 10px;",
            "  }",
            "  .table-wrap {",
            "    width: 100%;",
            "    overflow-x: auto;",
            "    -webkit-overflow-scrolling: touch;",
            "  }",
            "  .adaptive-table {",
            "    width: 100%;",
            "    min-width: 560px;",
            "    border-collapse: separate;",
            "    border-spacing: 0;",
            "    background-color: #ffffff;",
            "    border-radius: 12px;",
            "    overflow: hidden;",
            "    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);",
            "  }",
            "  .adaptive-table thead { background-color: #f5f5f7; }",
            "  .adaptive-table th {",
            "    padding: 12px 14px;",
            "    text-align: left;",
            "    font-size: 12px;",
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
            "    padding: 12px 14px;",
            "    border-bottom: 1px solid #f5f5f7;",
            "    border-right: 1px solid #e5e5e7;",
            "    font-size: 14px;",
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
            "    margin-top: 18px;",
            "    padding: 8px 6px;",
            "    background-color: #fafafa;",
            "    border-radius: 12px;",
            "    page-break-inside: avoid;",
            "  }",
            "  .graph-title {",
            "    font-size: 17px;",
            "    font-weight: 600;",
            "    letter-spacing: -0.3px;",
            "    color: #1d1d1f;",
            "    margin-bottom: 14px;",
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
            "    font-size: 13px;",
            "    padding: 12px 0;",
            "  }",
            "  .unsuccessful-list {",
            "    margin-top: 16px;",
            "    padding: 14px;",
            "    background-color: #fff5f5;",
            "    border-radius: 8px;",
            "    border-left: 3px solid #ff3b30;",
            "  }",
            "  .successful-list {",
            "    margin-top: 16px;",
            "    padding: 14px;",
            "    background-color: #ecfdf5;",
            "    border-radius: 8px;",
            "    border-left: 3px solid #2e7d32;",
            "  }",
            "  .unsuccessful-list strong {",
            "    color: #ff3b30;",
            "    font-size: 15px;",
            "    font-weight: 600;",
            "    display: block;",
            "    margin-bottom: 12px;",
            "  }",
            "  .successful-list strong {",
            "    color: #2e7d32;",
            "    font-size: 15px;",
            "    font-weight: 600;",
            "    display: block;",
            "    margin-bottom: 12px;",
            "  }",
            "  .unsuccessful-list ul {",
            "    margin: 0;",
            "    padding-left: 20px;",
            "    color: #1d1d1f;",
            "    font-size: 14px;",
            "  }",
            "  .successful-list ul {",
            "    margin: 0;",
            "    padding-left: 20px;",
            "    color: #1d1d1f;",
            "    font-size: 14px;",
            "  }",
            "  .unsuccessful-list li {",
            "    margin-bottom: 6px;",
            "  }",
            "  .chart-container {",
            "    margin-top: 24px;",
            "    padding: 8px 6px;",
            "    background-color: #fafafa;",
            "    border-radius: 12px;",
            "  }",
            "  body { padding: 10px 6px; }",
            "  .container { border-radius: 10px; }",
            "  .header { padding: 16px 14px 12px; }",
            "  .content { padding: 10px; }",
            "  .graph-section { padding: 8px 6px; }",
            "  .chart-container { padding: 8px 6px; }",
            "</style>",
            "</head>",
            "<body>",
            "<div class='container'>",
            "  <div class='header'>",
            f"    <h2>Сводка по станциям {date_display}</h2>",
            "  </div>",
            "  <div class='content'>"
        ]

        # Коммерческие пролеты (как в old_GroundLinkServer) — первым блоком
        if comm_stats is not None and comm_totals is not None:
            html_lines.append("    <h2 style='margin-top: 0; font-size: 24px; font-weight: 600; letter-spacing: -0.3px; color: #1d1d1f;'>Коммерческие пролеты</h2>")
            html_lines.append("    <div class='table-wrap'>")
            html_lines.append("      <table class='adaptive-table'>")
            html_lines.append("        <thead>")
            html_lines.append("          <tr>")
            html_lines.append("            <th>Станция</th>")
            html_lines.append("            <th class='number'>Всего</th>")
            html_lines.append("            <th class='number'>Успешных</th>")
            html_lines.append("            <th class='number'>Не принятых</th>")
            html_lines.append("            <th class='number'>% не принятых</th>")
            html_lines.append("          </tr>")
            html_lines.append("        </thead>")
            html_lines.append("        <tbody>")

            for station_name in sorted(comm_stats.keys()):
                stats = comm_stats[station_name]
                planned = int(stats.get("planned", 0))
                successful = int(stats.get("successful", 0))
                not_received = int(stats.get("not_received", 0))
                percent = (not_received / planned * 100) if planned > 0 else 0.0
                if planned == 0:
                    row_class = "row-good"
                elif percent <= 5:
                    row_class = "row-good"
                elif percent <= 25:
                    row_class = "row-warning"
                else:
                    row_class = "row-error"
                station_name_escaped = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                html_lines.append(f"        <tr class='{row_class}'>")
                html_lines.append(f"          <td>{station_name_escaped}</td>")
                html_lines.append(f"          <td class='number'>{planned}</td>")
                html_lines.append(f"          <td class='number'>{successful}</td>")
                html_lines.append(f"          <td class='number'>{not_received}</td>")
                html_lines.append(f"          <td class='number'>{percent:.1f}%</td>")
                html_lines.append("        </tr>")

            total_planned = int(comm_totals.get("planned", 0))
            total_successful = int(comm_totals.get("successful", 0))
            total_not_received = int(comm_totals.get("not_received", 0))
            total_percent = (total_not_received / total_planned * 100) if total_planned > 0 else 0.0
            html_lines.append("        <tr class='total-row'>")
            html_lines.append("          <td>ИТОГО</td>")
            html_lines.append(f"          <td class='number'>{total_planned}</td>")
            html_lines.append(f"          <td class='number'>{total_successful}</td>")
            html_lines.append(f"          <td class='number'>{total_not_received}</td>")
            html_lines.append(f"          <td class='number'>{total_percent:.1f}%</td>")
            html_lines.append("        </tr>")
            html_lines.append("        </tbody>")
            html_lines.append("      </table>")
            html_lines.append("    </div>")

            if comm_summary_7d_chart_path and Path(comm_summary_7d_chart_path).exists():
                comm_cid = "comm_unsuccessful_7d"
                inline_images[comm_cid] = Path(comm_summary_7d_chart_path)
                html_lines.append("    <div class='chart-container'>")
                html_lines.append(f"      <img src='cid:{comm_cid}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='Коммерческие пролеты: % не принятых за 7 дней' />")
                html_lines.append("    </div>")

            if comm_not_received_list:
                html_lines.append("    <div class='graph-section' style='margin-top:12px;'>")
                html_lines.append("      <div class='graph-title'>Не принятые коммерческие пролёты</div>")
                html_lines.append("      <ul style='margin:8px 0; padding-left:20px;'>")
                for item in comm_not_received_list:
                    station_name = item[0]
                    satellite_name = item[1]
                    rx_start = item[2]
                    rx_end = item[3]
                    graph_url = (item[4] if len(item) > 4 else "").strip()
                    station_esc = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    satellite_esc = satellite_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    label = f"{station_esc} — {satellite_esc}: {rx_start} – {rx_end}"
                    if graph_url:
                        url_esc = graph_url.replace("&", "&amp;").replace('"', "&quot;")
                        html_lines.append(f"        <li><a href='{url_esc}'>{label}</a></li>")
                    else:
                        html_lines.append(f"        <li>{label}</li>")
                html_lines.append("      </ul>")
                html_lines.append("    </div>")

            if comm_links:
                success_links = comm_links.get("successful") or []
                fail_links = comm_links.get("unsuccessful") or []
                html_lines.append("    <div class='graph-section'>")
                html_lines.append("      <div class='graph-title'>Графики коммерческих пролетов</div>")
                if success_links:
                    html_lines.append("      <div class='successful-list'>")
                    html_lines.append("        <strong>Успешные коммерческие пролеты:</strong>")
                    html_lines.append("        <ul>")
                    for url in success_links:
                        html_lines.append(f"          <li><a href='{url}'>{url}</a></li>")
                    html_lines.append("        </ul>")
                    html_lines.append("      </div>")
                if fail_links:
                    html_lines.append("      <div class='unsuccessful-list'>")
                    html_lines.append("        <strong>Неуспешные:</strong>")
                    html_lines.append("        <ul>")
                    for url in fail_links:
                        html_lines.append(f"          <li><a href='{url}'>{url}</a></li>")
                    html_lines.append("        </ul>")
                    html_lines.append("      </div>")
                html_lines.append("    </div>")

        # Общая статистика по станциям (как в old_GroundLinkServer)
        html_lines.append("    <h2 style='margin-top: 48px; font-size: 24px; font-weight: 600; letter-spacing: -0.3px; color: #1d1d1f;'>Общая статистика</h2>")
        html_lines.append("    <div class='table-wrap'>")
        html_lines.append("      <table class='adaptive-table'>")
        html_lines.append("        <thead>")
        html_lines.append("          <tr>")
        html_lines.append("            <th>Станция</th>")
        html_lines.append("            <th class='number'>Всего</th>")
        html_lines.append("            <th class='number'>Успешных</th>")
        html_lines.append("            <th class='number'>Пустых</th>")
        html_lines.append("            <th class='number'>% пустых</th>")
        html_lines.append("          </tr>")
        html_lines.append("        </thead>")
        html_lines.append("        <tbody>")

        for station_name, stats in sorted_stations:
            files = _files(stats)
            successful = _successful(stats)
            unsuccessful = _unsuccessful(stats)
            unsuccessful_percent = (unsuccessful / files * 100) if files > 0 else 0.0
            if files == 0:
                row_class = "row-error"
            elif unsuccessful_percent <= 5:
                row_class = "row-good"
            elif unsuccessful_percent <= 25:
                row_class = "row-warning"
            else:
                row_class = "row-error"
            station_name_escaped = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            html_lines.append(f"        <tr class='{row_class}'>")
            html_lines.append(f"          <td>{station_name_escaped}</td>")
            html_lines.append(f"          <td class='number'>{files}</td>")
            html_lines.append(f"          <td class='number'>{successful}</td>")
            html_lines.append(f"          <td class='number'>{unsuccessful}</td>")
            html_lines.append(f"          <td class='number'>{unsuccessful_percent:.1f}%</td>")
            html_lines.append("        </tr>")

        total_all_files = sum(_files(s) for s in all_results.values())
        total_successful = sum(_successful(s) for s in all_results.values())
        total_unsuccessful = sum(_unsuccessful(s) for s in all_results.values())
        total_unsuccessful_percent = (total_unsuccessful / total_all_files * 100) if total_all_files > 0 else 0.0

        html_lines.append("        <tr class='total-row'>")
        html_lines.append("          <td>ИТОГО</td>")
        html_lines.append(f"          <td class='number'>{total_all_files}</td>")
        html_lines.append(f"          <td class='number'>{total_successful}</td>")
        html_lines.append(f"          <td class='number'>{total_unsuccessful}</td>")
        html_lines.append(f"          <td class='number'>{total_unsuccessful_percent:.1f}%</td>")
        html_lines.append("        </tr>")
        html_lines.append("        </tbody>")
        html_lines.append("      </table>")
        html_lines.append("    </div>")

        if summary_7d_chart_path and Path(summary_7d_chart_path).exists():
            summary_cid = "summary_unsuccessful_7d"
            inline_images[summary_cid] = Path(summary_7d_chart_path)
            html_lines.append("    <div class='chart-container'>")
            html_lines.append(f"      <img src='cid:{summary_cid}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='Сводный график за 7 дней' />")
            html_lines.append("    </div>")
        else:
            html_lines.append("    <p class='empty-message'>Нет данных для построения графика.</p>")

        # Графики пролетов по станциям (как в old_GroundLinkServer): по 1 лучшему пролету с графиком + список пустых
        graphs_dir_p = Path(graphs_dir) if graphs_dir else None
        html_lines.append("    <h2 style='margin-top: 48px; font-size: 24px; font-weight: 600; letter-spacing: -0.3px; color: #1d1d1f;'>Графики пролетов</h2>")
        for station_name, stats in sorted_stations:
            station_name_escaped = station_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            html_lines.append("    <div class='graph-section'>")
            html_lines.append(f"      <div class='graph-title'>{station_name_escaped}</div>")
            max_snr_filename = stats.get("max_snr_filename", "")
            best_graph_path = stats.get("best_graph_path")
            graph_path = None
            if best_graph_path and Path(best_graph_path).exists():
                graph_path = Path(best_graph_path)
            elif max_snr_filename and graphs_dir_p and graphs_dir_p.exists():
                graph_name = max_snr_filename.replace(".log", ".png").replace(" ", "_")
                graph_path = graphs_dir_p / station_name / graph_name
                if not graph_path.exists():
                    graph_path = graphs_dir_p / graph_name
                if not graph_path.exists():
                    graph_path = None
            if graph_path and graph_path.exists():
                graph_name = graph_path.name
                cid = f"graph_{station_name}_{graph_name}".replace(" ", "_").replace(".", "_")
                inline_images[cid] = graph_path
                html_lines.append(f"      <img src='cid:{cid}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='График для {station_name_escaped}' />")
            elif max_snr_filename:
                html_lines.append("      <p class='empty-message'>График лучшего пролета не найден</p>")
            else:
                html_lines.append("      <p class='empty-message'>Нет данных по лучшему пролету</p>")
            # График % пустых за 7 дней по станции
            station_7d_path = stats.get("station_7d_chart_path")
            if station_7d_path and Path(station_7d_path).exists():
                station_7d_p = Path(station_7d_path)
                cid_7d = f"station_7d_{station_name}".replace(" ", "_").replace(".", "_")
                inline_images[cid_7d] = station_7d_p
                html_lines.append("      <p style='margin-top:12px;font-size:13px;color:#86868b;'>Пустые пролеты за 7 дней</p>")
                html_lines.append(f"      <img src='cid:{cid_7d}' class='graph-image' style='width:100%;max-width:100%;height:auto;display:block;' alt='% пустых за 7 дней — {station_name_escaped}' />")
            unsuccessful_filenames = stats.get("unsuccessful_filenames", [])
            if unsuccessful_filenames:
                html_lines.append("      <div class='unsuccessful-list'>")
                html_lines.append(f"        <strong>Пустые пролеты ({len(unsuccessful_filenames)})</strong>")
                html_lines.append("        <ul>")
                for item in unsuccessful_filenames:
                    s = str(item).strip()
                    if s.startswith("http://") or s.startswith("https://"):
                        link_url = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    else:
                        link_url = f"https://eus.lorett.org/eus/log_view/{quote(s)}".replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    html_lines.append(f"          <li><a href='{link_url}' target='_blank' rel='noopener noreferrer'>{link_url}</a></li>")
                html_lines.append("        </ul>")
                html_lines.append("      </div>")
            html_lines.append("    </div>")

        html_lines.extend([
            "  </div>",
            "</div>",
            "</body>",
            "</html>",
        ])
        return "\n".join(html_lines), inline_images

    # Отправляет письмо через SMTP с inline-изображениями и вложениями.
    def send_stats_email(
        self,
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
        """Отправляет письмо через SMTP с поддержкой HTML, inline-изображений и вложений.

        Использует SMTP_SSL для порта 465, starttls() для 587, обычный SMTP для остальных.

        Args:
            smtp_server: Адрес SMTP-сервера.
            smtp_port: Порт (465 — SSL, 587 — STARTTLS).
            sender_email: Адрес отправителя.
            sender_password: Пароль (или app password).
            recipients: Список получателей.
            cc_recipients: Список получателей в копии.
            subject: Тема письма.
            body: Тело (HTML или plain text).
            attachments: Пути к файлам для вложений.
            inline_images: {content_id: Path} — изображения для вставки в body по cid:content_id.

        Returns:
            True при успешной отправке, False при ошибке или отсутствии получателей.
        """
        if not sender_email or not sender_password or not recipients:
            self.logger.warning("Email: не заданы sender/password/recipients — отправка пропущена")
            return False

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = ", ".join(recipients)
        if cc_recipients:
            msg["Cc"] = ", ".join([r for r in cc_recipients if r])
        msg["Subject"] = subject
        is_html = body.strip().startswith("<!DOCTYPE html>") or body.strip().startswith("<html>")
        msg.attach(MIMEText(body, "html" if is_html else "plain", "utf-8"))

        if inline_images:
            for cid, image_path in inline_images.items():
                try:
                    if not image_path or not Path(image_path).exists():
                        self.logger.warning(f"Email: график не найден {image_path}")
                        continue
                    with open(image_path, "rb") as f:
                        img = MIMEImage(f.read())
                    img.add_header("Content-ID", f"<{cid}>")
                    img.add_header("Content-Disposition", "inline", filename=Path(image_path).name)
                    msg.attach(img)
                except Exception as e:
                    self.logger.warning(f"Email: не удалось приложить график {image_path}: {e}")

        for p in attachments or []:
            try:
                if not p or not Path(p).exists():
                    continue
                with open(p, "rb") as f:
                    part = MIMEApplication(f.read(), Name=Path(p).name)
                part["Content-Disposition"] = f'attachment; filename="{Path(p).name}"'
                msg.attach(part)
            except Exception as e:
                self.logger.warning(f"Email: не удалось приложить файл {p}: {e}")

        try:
            if int(smtp_port) == 465:
                server = smtplib.SMTP_SSL(smtp_server, int(smtp_port), timeout=30)
            else:
                server = smtplib.SMTP(smtp_server, int(smtp_port), timeout=30)
                if int(smtp_port) == 587:
                    server.starttls()
            server.login(sender_email, sender_password)
            all_recipients: List[str] = list(recipients or [])
            if cc_recipients:
                all_recipients.extend([r for r in cc_recipients if r])
            server.send_message(msg, from_addr=sender_email, to_addrs=all_recipients)
            server.quit()
            return True
        except Exception as e:
            self.logger.exception(f"Email: ошибка отправки: {e}")
            return False
