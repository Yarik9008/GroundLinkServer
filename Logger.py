import logging
import coloredlogs
from datetime import datetime
import os


class Logger:
    """Обёртка для ведения логов: файл + консоль с цветами.

    Использует стандартный logging и coloredlogs. Файл: path_log + logger_name-YYYY-MM-DD.log.

    Методы:
        __init__: Инициализация с путём, уровнем и именем.
        debug, info, warning, error, critical: Логирование по уровням.
        exception: Логирование исключения с трассировкой.

    Атрибуты:
        logs: Объект logging.Logger.
    """

    # Инициализация логгера с путём, уровнем и именем.
    def __init__(self, path_log, log_level='info', logger_name=None):
        """Инициализирует логгер.

        Args:
            path_log: Базовый путь (с завершающим разделителем). Файл: path_log + logger_name-YYYY-MM-DD.log.
            log_level: Уровень логирования (debug, info, warning, error, critical).
            logger_name: Имя логгера для файла и консоли; если None — __name__.
        """
        
        # Соответствие строковых уровней и констант logging.
        log_level_map = {'debug':logging.DEBUG,
                         'info':logging.INFO,
                         'warning':logging.WARNING,
                         'critical':logging.CRITICAL,
                         'error':logging.ERROR}

        # Имя логгера: берём указанное или имя текущего модуля.
        logger_name = logger_name or __name__
        self.logs = logging.getLogger(logger_name)
        if self.logs.handlers:
            self.logs.handlers.clear()
        self.logs.setLevel(log_level_map[log_level])

        # Проверяем, что директория path_log существует.
        if path_log:
            dir_path = os.path.dirname(path_log)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)

        # Имя лог-файла: путь + имя_логгера + дата (YYYY-MM-DD) + .log
        name = path_log + logger_name + '-' + datetime.now().strftime("%Y-%m-%d") + '.log'

        # Обработчик для записи в файл.
        self.file = logging.FileHandler(name, encoding="utf-8")
        self.fileformat = logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s")
        self.file.setLevel(log_level_map[log_level])
        self.file.setFormatter(self.fileformat)

        # Обработчик для вывода в консоль.
        self.stream = logging.StreamHandler()
        self.streamformat = logging.Formatter(
            "%(levelname)s:%(name)s:%(message)s")
        self.stream.setLevel(log_level_map[log_level])
        self.stream.setFormatter(self.streamformat)

        # Регистрируем обработчики и включаем цветной вывод.
        self.logs.addHandler(self.file)
        self.logs.addHandler(self.stream)
        coloredlogs.install(
            level=log_level_map[log_level],
            logger=self.logs,
            fmt="%(asctime)s : %(levelname)s : %(name)s : %(message)s",
        )

        # Стартовое сообщение для проверки работы логгера.
        self.logs.info('Start logging')

    # Логирование отладочного сообщения.
    def debug(self, message):
        """Логирует отладочное сообщение."""
        self.logs.debug(message)

    # Логирование информационного сообщения.
    def info(self, message):
        """Логирует информационное сообщение."""
        self.logs.info(message)

    # Логирование предупреждения.
    def warning(self, message):
        """Логирует предупреждение."""
        self.logs.warning(message)

    # Логирование критической ошибки.
    def critical(self, message):
        """Логирует критическое сообщение."""
        self.logs.critical(message)

    # Логирование исключения с трассировкой стека.
    def exception(self, message, exc_info=None):
        """Логирует исключение с трассировкой стека.

        Args:
            message: Сообщение.
            exc_info: Передать в logging.exception (None — авто-определение из текущего except).
        """
        self.logs.exception(message, exc_info=exc_info)

    # Логирование сообщения об ошибке.
    def error(self, message):
        """Логирует сообщение об ошибке."""
        self.logs.error(message)
