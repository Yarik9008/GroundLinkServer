from verboselogs import VerboseLogger, SPAM, NOTICE, VERBOSE
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from datetime import datetime
from os.path import exists, join
from os import mkdir

from time import gmtime

import logging
import coloredlogs



loggingLevels = {
    "spam": SPAM,
    "debug": DEBUG,
    "verbose": VERBOSE,
    "info": INFO,
    "warning": WARNING,
    "error": ERROR,
    "critical": CRITICAL,
    SPAM: "spam",
    DEBUG: "debug",
    VERBOSE: "verbose",
    INFO: "info",
    WARNING: "warning",
    ERROR: "error",
    CRITICAL: "critical"
}



class Logger:
    '''Класс отвечающий за логирование. Логи пишуться в файл, так же выводться в консоль'''
    def __init__(self, name: str, path: str, level: int) -> None:
        
        self.mylogs = VerboseLogger(__name__)
        self.level = level

        self.mylogs.setLevel(self.level)

        self.path = path

        if self.path != "":
            if not exists(self.path):
                mkdir(self.path)

        # обработчик записи в лог-файл
        fileName = datetime.utcnow().strftime(f"{name}_%d-%m-%Y") + ".log"
        fileName = join(self.path, fileName)

        self.file = logging.FileHandler(fileName)
        self.fileformat = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")

        logging.Formatter.converter = gmtime

        self.file.setLevel(level)
        self.file.setFormatter(self.fileformat)

        # обработчик вывода в консоль лог файла
        self.stream = logging.StreamHandler()
        self.streamformat = logging.Formatter("%(levelname)s:%(module)s:%(message)s")

        self.stream.setLevel(level)
        self.stream.setFormatter(self.streamformat)

        # инициализация обработчиков
        self.mylogs.addHandler(self.file)
        self.mylogs.addHandler(self.stream)

        coloredlogs.install(level=level, logger=self.mylogs, fmt='%(asctime)s [%(levelname)s] %(message)s')

        logging.Formatter.converter = gmtime                            

        self.lastLog = {"message": " ", "source": "station"}

        self.mylogs.info('Start Logger')

    def spam(self, message: str, source: str = "station") -> None:
        
        if self.level <= SPAM:
            self.lastLog["message"] = message
            self.lastLog["source"] = source

        self.mylogs.spam(message)
    
    def verbose(self, message: str, source: str = "station") -> None:
        if self.level <= VERBOSE:
            self.lastLog["message"] = message
            self.lastLog["source"] = source

        self.mylogs.verbose(message)

    def notice(self, message: str, source: str = "station") -> None:
        if self.level <= NOTICE:
            self.lastLog["message"] = message
            self.lastLog["source"] = source

        self.mylogs.notice(message)

    def debug(self, message: str, source: str = "station") -> None:
        if self.level <= DEBUG:
            self.lastLog["message"] = message
            self.lastLog["source"] = source
                            
        self.mylogs.debug(message)


    def info(self, message: str, source: str = "station") -> None:
        if self.level <= INFO:
            self.lastLog["message"] = message
            self.lastLog["source"] = source

        self.mylogs.info(message)

    def warning(self, message: str, source: str = "station") -> None:
        if self.level <= WARNING:
            self.lastLog["message"] = message
            self.lastLog["source"] = source

        self.mylogs.warning(message)

    def critical(self, message: str, source: str = "station") -> None:
        if self.level <= CRITICAL:
            self.lastLog["message"] = message
            self.lastLog["source"] = source

        self.mylogs.critical(message)
        exit(-1) 

    def error(self, message: str, source: str = "station") -> None:
        if self.level <= ERROR:
            self.lastLog["message"] = message
            self.lastLog["source"] = source
                        
        self.mylogs.error(message)
