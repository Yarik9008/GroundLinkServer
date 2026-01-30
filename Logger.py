import logging
import coloredlogs
from datetime import datetime
import os


class Logger:
    '''Класс для ведения логов. Записи сохраняются в файл и дублируются в консоль.'''

    def __init__(self, path_log, log_level='info', logger_name=None):
        """Инициализирует логгер.

        Args:
            path_log: Базовый путь/префикс имени лог-файла.
            log_level: Уровень логирования строкой (debug, info, warning, error, critical).
            logger_name: Имя логгера; если не задано, используется имя модуля.
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

        # Имя лог-файла с меткой текущей даты и времени.
        name = path_log + logger_name + '-' + '-'.join('-'.join('-'.join(str(datetime.now()).split()).split('.')).split(':')) + '.log'

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

    def debug(self, message):
        """Логирует отладочное сообщение."""
        self.logs.debug(message)

    def info(self, message):
        """Логирует информационное сообщение."""
        self.logs.info(message)

    def warning(self, message):
        """Логирует предупреждение."""
        self.logs.warning(message)

    def critical(self, message):
        """Логирует критическое сообщение."""
        self.logs.critical(message)
    
    def exception(self, message, exc_info=None):
        """Логирует исключение."""
        self.logs.exception(message, exc_info=exc_info)

    def error(self, message):
        """Логирует сообщение об ошибке."""
        self.logs.error(message)
