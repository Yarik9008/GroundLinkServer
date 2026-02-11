from __future__ import annotations
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class SatPas:
    """Данные о пролёте спутника через станцию.

    Все поля опциональны, заполняются по мере получения данных (парсинг,
    загрузка, анализ). Используется для записи в БД и передачи в EmailClient.

    Методы:
        __init__: Создание записи пролёта.
        __str__: Человекочитаемое представление.
        __repr__: Компактное представление для логов.

    Атрибуты:
        pass_id: ID пролёта.
        station_name: Название наземной станции.
        satellite_name: Название спутника.
        location: Местоположение станции.
        pass_date: Дата пролёта.
        pass_start_time, pass_end_time: Время начала/окончания пролёта.
        rx_start_time, rx_end_time: Время начала/окончания приёма.
        snr_awg, snr_max, snr_sum: Средний, максимальный, суммарный SNR.
        log_url, log_path: URL и путь к логу.
        graph_url, graph_path: URL и путь к графику.
        success: Признак успешного пролёта.
    """

    # Создание записи пролёта с опциональными полями.
    def __init__(
        self,
        pass_id: str = "",
        station_name: str = "",
        satellite_name: str = "",
        location: Optional[str] = None,
        pass_date: Optional[date] = None,
        pass_start_time: Optional[datetime] = None,
        pass_end_time: Optional[datetime] = None,
        rx_start_time: Optional[datetime] = None,
        rx_end_time: Optional[datetime] = None,
        snr_awg: Optional[float] = None,
        snr_max: Optional[float] = None,
        snr_sum: Optional[float] = None,
        log_url: Optional[str] = None,
        log_path: Optional[str] = None,
        graph_url: Optional[str] = None,
        graph_path: Optional[str] = None,
        success: bool = False,
        ) -> None:

        """Создаёт запись пролёта спутника.

        Args:
            pass_id: ID пролёта.
            station_name: Название станции.
            satellite_name: Название спутника.
            location: Местоположение.
            pass_date: Дата пролёта.
            pass_start_time, pass_end_time: Время пролёта.
            rx_start_time, rx_end_time: Время приёма.
            snr_awg, snr_max, snr_sum: SNR-метрики.
            log_url, log_path: Ссылки на лог.
            graph_url, graph_path: Ссылки на график.
            success: Успешность пролёта.
        """
        self.pass_id = pass_id
        # Название наземной станции
        self.station_name = station_name
        # Название спутника
        self.satellite_name = satellite_name
        # Местоположение станции
        self.location = location
        # Дата пролета
        self.pass_date = pass_date
        # Время начала пролета
        self.pass_start_time = pass_start_time
        # Время окончания пролета
        self.pass_end_time = pass_end_time
        # Время начала приема
        self.rx_start_time = rx_start_time
        # Время окончания приема
        self.rx_end_time = rx_end_time
        # Средний SNR
        self.snr_awg = snr_awg
        # Максимальный SNR
        self.snr_max = snr_max
        # Сумма SNR
        self.snr_sum = snr_sum
        # URL лога
        self.log_url = log_url
        # Локальный путь к логу
        self.log_path = log_path
        # URL графика
        self.graph_url = graph_url
        # Локальный путь к графику
        self.graph_path = graph_path
        # Признак успешного пролета
        self.success = success

    # Человекочитаемое представление (многострочное).
    def __str__(self) -> str:
        """Человекочитаемое представление пролета."""
        return (
            "SatPas(\n"
            f"  pass_id={self.pass_id!r},\n"
            f"  station_name={self.station_name!r},\n"
            f"  satellite_name={self.satellite_name!r},\n"
            f"  location={self.location!r},\n"
            f"  pass_date={self.pass_date!r},\n"
            f"  pass_start_time={self.pass_start_time!r},\n"
            f"  pass_end_time={self.pass_end_time!r},\n"
            f"  rx_start_time={self.rx_start_time!r},\n"
            f"  rx_end_time={self.rx_end_time!r},\n"
            f"  snr_awg={self.snr_awg!r},\n"
            f"  snr_max={self.snr_max!r},\n"
            f"  snr_sum={self.snr_sum!r},\n"
            f"  log_url={self.log_url!r},\n"
            f"  log_path={self.log_path!r},\n"
            f"  graph_url={self.graph_url!r},\n"
            f"  graph_path={self.graph_path!r},\n"
            f"  success={self.success!r}\n"
            ")"
        )

    # Компактное представление для логов и отладки.
    def __repr__(self) -> str:
        """Представление для логов и списков."""
        return (
            "SatPas("
            f"pass_id={self.pass_id!r},\n"
            f"station_name={self.station_name!r}, "
            f"satellite_name={self.satellite_name!r}, "
            f"location={self.location!r}, "
            f"pass_date={self.pass_date!r}, "
            f"pass_start_time={self.pass_start_time!r}, "
            f"pass_end_time={self.pass_end_time!r}, "
            f"rx_start_time={self.rx_start_time!r}, "
            f"rx_end_time={self.rx_end_time!r}, "
            f"snr_awg={self.snr_awg!r}, "
            f"snr_max={self.snr_max!r}, "
            f"snr_sum={self.snr_sum!r}, "
            f"log_url={self.log_url!r}, "
            f"log_path={self.log_path!r}, "
            f"graph_url={self.graph_url!r}, "
            f"graph_path={self.graph_path!r}, "
            f"success={self.success!r}"
            ")\n"
        )


if __name__ == "__main__":
    # Мини-тест создания нескольких экземпляров
    BASE_DIR = Path(__file__).resolve().parent
    default_pass = SatPas()
    filled_pass = SatPas(
        pass_id="20260127_031121_FENGYUN 3D",
        station_name="GS-1",
        satellite_name="SAT-A",
        location="177.4865 lon 64.73178 lat",
        pass_date=date(2026, 1, 26),
        pass_start_time=datetime(2026, 1, 26, 12, 30),
        pass_end_time=datetime(2026, 1, 26, 12, 45),
        rx_start_time=datetime(2026, 1, 26, 12, 31),
        rx_end_time=datetime(2026, 1, 26, 12, 44),
        snr_awg=12.5,
        snr_max=18.2,
        snr_sum=100.0,
        log_url="https://example.com/log",
        log_path=str(BASE_DIR / "logs" / "sat-a.log"),
        graph_url="https://example.com/graph",
        graph_path=str(BASE_DIR / "graphs" / "sat-a.png"),
        success=True,
    )
    print(default_pass)
    print(filled_pass)