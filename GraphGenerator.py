import logging
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Any, List, Optional, Set, Tuple


class GraphGenerator:
    """Генератор PNG-графиков по данным из БД.

    Строит линейные графики % пустых пролётов и % не принятых коммерческих
    за последние N дней. Требует у db_manager методы:
        - get_daily_success_stats(stat_day) → [[station, total, success, failed, failed_percent], ...]
        - get_commercial_passes_stats_by_station(day, up_to_datetime) → (stats, totals)

    Методы:
        __init__: Инициализация с db_manager и логгером.
        _ensure_matplotlib: Импорт matplotlib в режиме Agg.
        _plot_unsuccessful_7d: Отрисовка линейного графика % по дням.
        generate_overall_unsuccessful_7d: График % пустых за 7 дней (все станции или по фильтру).
        generate_comm_unsuccessful_7d: График % не принятых коммерческих за 7 дней.
        generate_station_unsuccessful_7d: График % пустых за 7 дней по одной станции.

    Атрибуты:
        db_manager: Менеджер БД для получения статистики.
        logger: Логгер для предупреждений и ошибок.
    """

    # Инициализация генератора с db_manager и логгером.
    def __init__(self, db_manager: Any, logger: Optional[Any] = None) -> None:
        """Инициализирует генератор.

        Args:
            db_manager: Менеджер БД (DbManager) с методами get_daily_success_stats,
                        get_commercial_passes_stats_by_station.
            logger: Опциональный логгер для сообщений об ошибках.
        """
        self.db_manager = db_manager
        self.logger = logger

    # Импорт matplotlib в non-GUI режиме (Agg).
    def _ensure_matplotlib(self) -> bool:
        """Проверяет доступность matplotlib и настраивает backend Agg.

        Returns:
            True если matplotlib доступен, False при ImportError.
        """
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt  # type: ignore
            logging.getLogger("matplotlib").setLevel(logging.WARNING)
            return True
        except ImportError:
            if self.logger:
                self.logger.warning("matplotlib not installed — graphs skipped")
            return False

    # Отрисовка линейного графика % по дням и сохранение в PNG.
    def _plot_unsuccessful_7d(
        self,
        points: List[Tuple[str, Optional[float]]],
        output_path: Path,
        title: str,
        figsize: Tuple[float, float] = (10, 3.2),
        dpi: int = 150,
        no_data_label: str = "нет\nданных",
        ylabel: str = "% пустых",
        ) -> Optional[Path]:
        """Рисует линейный график (дата → %) и сохраняет в output_path.

        Args:
            points: Список (метка_дня, значение_%) или (метка, None) для пропуска.
            output_path: Путь к выходному PNG.
            title: Заголовок графика.
            figsize: Размер фигуры (ширина, высота) в дюймах.
            dpi: Разрешение для сохранения.
            no_data_label: Подпись для точек без данных.
            ylabel: Подпись оси Y.

        Returns:
            Path к сохранённому файлу или None при ошибке.
        """
        try:
            import matplotlib.pyplot as plt  # type: ignore
        except ImportError:
            return None

        labels = [p[0] for p in points]
        values = [(p[1] if p[1] is not None else float("nan")) for p in points]
        x = list(range(len(labels)))

        fig = plt.figure(figsize=figsize, dpi=dpi)
        ax = fig.add_subplot(111)
        ax.plot(x, values, color="#1976d2", linewidth=2, marker="o", markersize=4)
        ax.set_xticks(x, labels)
        ax.set_ylim(0, 100)
        ax.set_yticks(list(range(0, 101, 10)))
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(axis="y", linestyle="--", alpha=0.3)
        for idx, (lbl, v) in enumerate(points):
            if v is None:
                ax.text(x[idx], 0.5, no_data_label, ha="center", va="bottom", fontsize=7, color="#616161")
            else:
                ax.text(x[idx], min(99.5, v + 1.5), f"{v:.1f}%", ha="center", va="bottom", fontsize=7, color="#212121")
        fig.tight_layout()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if output_path.exists():
                output_path.unlink()
        except OSError as exc:
            if self.logger:
                self.logger.warning(f"failed to remove old graph: {output_path} ({exc})")
        fig.savefig(output_path, bbox_inches="tight")
        plt.close(fig)
        return output_path

    # График % пустых пролётов за 7 дней (все станции или по фильтру).
    def generate_overall_unsuccessful_7d(
        self,
        target_date: str,
        output_path: Path,
        days: int = 7,
        stations_filter: Optional[Set[str]] = None,
        ) -> Optional[Path]:
        """
        Генерирует PNG график процента пустых пролетов за последние N дней (все станции или по фильтру).

        Args:
            target_date: Дата в формате YYYYMMDD.
            output_path: Путь для сохранения PNG.
            days: Количество дней (по умолчанию 7).
            stations_filter: Если задан — считаются только эти станции.

        Returns:
            Путь к сохранённому файлу или None при ошибке.
        """
        if not self._ensure_matplotlib():
            return None

        try:
            date_end = datetime.strptime(target_date, "%Y%m%d").date()
            points: List[Tuple[str, Optional[float]]] = []
            for i in range(days - 1, -1, -1):
                day = date_end - timedelta(days=i)
                rows = self.db_manager.get_daily_success_stats(day)
                if stations_filter:
                    filtered = [r for r in rows if r[0] != "total" and r[0] in stations_filter]
                    total_all = sum(int(r[1] or 0) for r in filtered)
                    failed_all = sum(int(r[3] or 0) for r in filtered)
                    if total_all and total_all > 0:
                        points.append((day.strftime("%d.%m"), (failed_all / total_all) * 100.0))
                    else:
                        points.append((day.strftime("%d.%m"), None))
                elif rows and rows[-1][0] == "total":
                    _, total_all, _, failed_all, failed_percent = rows[-1]
                    if total_all and total_all > 0:
                        points.append((day.strftime("%d.%m"), float(failed_percent)))
                    else:
                        points.append((day.strftime("%d.%m"), None))
                else:
                    points.append((day.strftime("%d.%m"), None))

            title_suffix = " (станции из письма)" if stations_filter else " (все станции)"
            title = "Общий % пустых пролетов за последние 7 дней" + title_suffix
            return self._plot_unsuccessful_7d(points, output_path, title)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"failed to generate 7d graph: {e}")
            return None

    # График % не принятых коммерческих пролётов за 7 дней.
    def generate_comm_unsuccessful_7d(
        self,
        target_date: str,
        output_path: Path,
        days: int = 7,
        up_to_datetime: Optional[datetime] = None,
        ) -> Optional[Path]:
        """Генерирует PNG график % не принятых коммерческих пролётов за N дней.

        Args:
            target_date: Дата в формате YYYYMMDD (конец периода).
            output_path: Путь для сохранения PNG.
            days: Количество дней (по умолчанию 7).
            up_to_datetime: Для целевой даты — учитывать только пролёты до этого момента.

        Returns:
            Путь к сохранённому файлу или None при ошибке.
        """
        if not self._ensure_matplotlib():
            return None
        try:
            date_end = datetime.strptime(target_date, "%Y%m%d").date()
            points: List[Tuple[str, Optional[float]]] = []
            for i in range(days - 1, -1, -1):
                day = date_end - timedelta(days=i)
                if up_to_datetime is not None and day == date_end:
                    end_of_day = up_to_datetime
                else:
                    end_of_day = datetime.combine(day, time(23, 59, 59))
                _, totals = self.db_manager.get_commercial_passes_stats_by_station(
                    day, up_to_datetime=end_of_day
                )
                planned = int(totals.get("planned", 0) or 0)
                not_received = int(totals.get("not_received", 0) or 0)
                if planned > 0:
                    percent = (not_received / planned) * 100.0
                    points.append((day.strftime("%d.%m"), percent))
                else:
                    points.append((day.strftime("%d.%m"), None))
            title = "Коммерческие пролеты: % не принятых за последние 7 дней"
            return self._plot_unsuccessful_7d(
                points,
                output_path,
                title,
                no_data_label="нет\nданных",
                ylabel="% не принятых",
            )
        except Exception as e:
            if self.logger:
                self.logger.warning(f"failed to generate comm 7d graph: {e}")
            return None

    # График % пустых пролётов за 7 дней по одной станции.
    def generate_station_unsuccessful_7d(
        self,
        station_name: str,
        target_date: str,
        output_path: Path,
        days: int = 7,
        ) -> Optional[Path]:
        """
        Генерирует PNG график процента пустых пролетов за последние N дней по одной станции.

        Args:
            station_name: Название станции.
            target_date: Дата в формате YYYYMMDD.
            output_path: Путь для сохранения PNG.
            days: Количество дней (по умолчанию 7).

        Returns:
            Путь к сохранённому файлу или None при ошибке.
        """
        if not self._ensure_matplotlib():
            return None

        try:
            date_end = datetime.strptime(target_date, "%Y%m%d").date()
            points: List[Tuple[str, Optional[float]]] = []
            for i in range(days - 1, -1, -1):
                day = date_end - timedelta(days=i)
                rows = self.db_manager.get_daily_success_stats(day)
                row_station = next((r for r in rows if r[0] == station_name), None)
                if row_station:
                    _, total, _, failed, failed_percent = row_station
                    if total and int(total) > 0:
                        points.append((day.strftime("%d.%m"), float(failed_percent)))
                    else:
                        points.append((day.strftime("%d.%m"), None))
                else:
                    points.append((day.strftime("%d.%m"), None))

            title = f"{station_name}: % пустых пролетов за 7 дней"
            return self._plot_unsuccessful_7d(
                points,
                output_path,
                title,
                figsize=(10, 2.8),
                dpi=120,
                no_data_label="нет",
            )
        except Exception as e:
            if self.logger:
                self.logger.warning(f"failed to generate 7d graph for {station_name}: {e}")
            return None
