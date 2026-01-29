import os
import re
import smtplib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage

from Logger import Logger


class EmailClient:
    """Отправка писем со статистикой по шаблону.

    Основано на логике из test/old_GroundLinkServer.py:
        - загрузка настроек SMTP,
        - формирование HTML-шаблона письма,
        - отправка письма с вложениями и inline-изображениями.
    """

    def __init__(self, logger: Logger) -> None:
        if logger is None:
            raise ValueError("logger is required")
        self.logger = logger

    def _load_email_defaults_from_test_email(self) -> Dict[str, Any]:
        """Пытается загрузить SMTP-настройки по умолчанию из test_email.py."""
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

    def get_email_settings(self, config: Dict[str, Any], debug_recipient: Optional[str] = None) -> Dict[str, Any]:
        """Возвращает настройки email с учетом config/env/test_email."""
        defaults = self._load_email_defaults_from_test_email()
        email_cfg = (config or {}).get("email", {}) if isinstance(config, dict) else {}

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
    ) -> Tuple[str, Dict[str, Path]]:
        """Формирует HTML письмо и набор inline-изображений."""
        date_display = f"{target_date[6:8]}.{target_date[4:6]}.{target_date[0:4]}"

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
            "  .metrics-table td:last-child {",
            "    text-align: right;",
            "    font-variant-numeric: tabular-nums;",
            "  }",
            "  .metrics-table tr:last-child td {",
            "    border-bottom: none;",
            "  }",
            "  .section {",
            "    margin: 16px 0 0 0;",
            "  }",
            "  .section-title {",
            "    margin: 0 0 12px 2px;",
            "    font-size: 16px;",
            "    font-weight: 600;",
            "    color: #1d1d1f;",
            "  }",
            "  .note {",
            "    font-size: 13px;",
            "    color: #6e6e73;",
            "    margin: 8px 0 0 2px;",
            "  }",
            "  .graph {",
            "    width: 100%;",
            "    max-width: 780px;",
            "    display: block;",
            "    margin: 12px auto;",
            "    border-radius: 12px;",
            "    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.06);",
            "  }",
            "</style>",
            "</head>",
            "<body>",
            "<div class='container'>",
            "  <div class='header'>",
            "    <h2>Ежедневный отчет</h2>",
            "    <p class='date'>" + date_display + "</p>",
            "  </div>",
            "  <div class='content'>",
        ]

        inline_images: Dict[str, Path] = {}

        html_lines.append("    <div class='section'>")
        html_lines.append("      <div class='section-title'>Статистика по станциям</div>")
        html_lines.append("      <div class='table-wrap'>")
        html_lines.append("        <table class='adaptive-table'>")
        html_lines.append("          <thead>")
        html_lines.append("            <tr>")
        html_lines.append("              <th>Станция</th>")
        html_lines.append("              <th class='number'>Файлов</th>")
        html_lines.append("              <th class='number'>Успешно</th>")
        html_lines.append("              <th class='number'>Неуспешно</th>")
        html_lines.append("              <th class='number'>%</th>")
        html_lines.append("              <th class='number'>Средний SNR</th>")
        html_lines.append("            </tr>")
        html_lines.append("          </thead>")
        html_lines.append("          <tbody>")

        total_files = 0
        total_unsuccessful = 0

        for station_name, stats in all_results.items():
            total = stats.get("total_files", 0)
            unsuccessful = stats.get("unsuccessful_passes", 0)
            successful = total - unsuccessful
            percent = (unsuccessful * 100.0 / total) if total > 0 else 0.0
            avg_snr = stats.get("avg_snr", 0.0)

            total_files += int(total)
            total_unsuccessful += int(unsuccessful)

            row_class = ""
            if percent >= 50:
                row_class = "row-error"
            elif percent >= 20:
                row_class = "row-warning"
            else:
                row_class = "row-good"

            html_lines.append(f"            <tr class='{row_class}'>")
            html_lines.append(f"              <td>{station_name}</td>")
            html_lines.append(f"              <td class='number'>{total}</td>")
            html_lines.append(f"              <td class='number'>{successful}</td>")
            html_lines.append(f"              <td class='number'>{unsuccessful}</td>")
            html_lines.append(f"              <td class='number'>{percent:.1f}%</td>")
            html_lines.append(f"              <td class='number'>{avg_snr:.2f}</td>")
            html_lines.append("            </tr>")

        total_successful = total_files - total_unsuccessful
        total_percent = (total_unsuccessful * 100.0 / total_files) if total_files > 0 else 0.0
        html_lines.append("            <tr class='total-row'>")
        html_lines.append("              <td>Итого</td>")
        html_lines.append(f"              <td class='number'>{total_files}</td>")
        html_lines.append(f"              <td class='number'>{total_successful}</td>")
        html_lines.append(f"              <td class='number'>{total_unsuccessful}</td>")
        html_lines.append(f"              <td class='number'>{total_percent:.1f}%</td>")
        html_lines.append("              <td class='number'>—</td>")
        html_lines.append("            </tr>")

        html_lines.append("          </tbody>")
        html_lines.append("        </table>")
        html_lines.append("      </div>")
        html_lines.append("    </div>")

        if summary_7d_chart_path:
            cid = "summary_7d_chart"
            inline_images[cid] = Path(summary_7d_chart_path)
            html_lines.append("    <div class='section'>")
            html_lines.append("      <div class='section-title'>Динамика пустых пролетов (7 дней)</div>")
            html_lines.append(f"      <img class='graph' src='cid:{cid}' alt='summary_7d_chart'>")
            html_lines.append("    </div>")

        if comm_stats and comm_totals:
            html_lines.append("    <div class='section'>")
            html_lines.append("      <div class='section-title'>Коммерческие пролеты</div>")
            html_lines.append("      <div class='table-wrap'>")
            html_lines.append("        <table class='summary-table'>")
            html_lines.append("          <thead>")
            html_lines.append("            <tr>")
            html_lines.append("              <th>Станция</th>")
            html_lines.append("              <th class='number'>X</th>")
            html_lines.append("              <th class='number'>L</th>")
            html_lines.append("              <th class='number'>Итого</th>")
            html_lines.append("            </tr>")
            html_lines.append("          </thead>")
            html_lines.append("          <tbody>")

            total_comm = 0
            total_comm_x = 0
            total_comm_l = 0

            for station_name, s in comm_stats.items():
                x_count = int(s.get("X", 0))
                l_count = int(s.get("L", 0))
                total_count = int(comm_totals.get(station_name, 0))
                total_comm += total_count
                total_comm_x += x_count
                total_comm_l += l_count

                html_lines.append("            <tr>")
                html_lines.append(f"              <td>{station_name}</td>")
                html_lines.append(f"              <td class='number'>{x_count}</td>")
                html_lines.append(f"              <td class='number'>{l_count}</td>")
                html_lines.append(f"              <td class='number'>{total_count}</td>")
                html_lines.append("            </tr>")

            html_lines.append("            <tr class='total-row'>")
            html_lines.append("              <td>Итого</td>")
            html_lines.append(f"              <td class='number'>{total_comm_x}</td>")
            html_lines.append(f"              <td class='number'>{total_comm_l}</td>")
            html_lines.append(f"              <td class='number'>{total_comm}</td>")
            html_lines.append("            </tr>")

            html_lines.append("          </tbody>")
            html_lines.append("        </table>")
            html_lines.append("      </div>")
            html_lines.append("    </div>")

        if comm_summary_7d_chart_path:
            cid = "comm_summary_7d_chart"
            inline_images[cid] = Path(comm_summary_7d_chart_path)
            html_lines.append("    <div class='section'>")
            html_lines.append("      <div class='section-title'>Коммерческие пролеты (7 дней)</div>")
            html_lines.append(f"      <img class='graph' src='cid:{cid}' alt='comm_summary_7d_chart'>")
            html_lines.append("    </div>")

        if comm_links:
            html_lines.append("    <div class='section'>")
            html_lines.append("      <div class='section-title'>Ссылки на коммерческие логи</div>")
            for station_name, links in comm_links.items():
                html_lines.append("      <div style='margin-bottom: 12px;'>")
                html_lines.append(f"        <div class='station-name'>{station_name}</div>")
                html_lines.append("        <ul>")
                for log_url in links:
                    log_url_escaped = log_url.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    html_lines.append(
                        f"          <li><a href='{log_url_escaped}' target='_blank' rel='noopener noreferrer'>{log_url_escaped}</a></li>"
                    )
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
        """Отправляет письмо со статистикой через SMTP."""
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
            self.logger.warning(f"Email: ошибка отправки: {e}", exc_info=True)
            return False
