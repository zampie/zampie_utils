import logging
from colorlog import ColoredFormatter


class Singleton(type):
    """
    description:
    param:
    return:
    """

    def __init__(self, *args, **kwargs):
        """
        description:
        param:
        return:
        """

        self.__instance = None
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        """
        description:
        param:
        return:
        """
        if self.__instance is None:
            self.__instance = super().__call__(*args, **kwargs)
        return self.__instance


class Logger(metaclass=Singleton):
    """"""

    def __init__(self, log_level=logging.INFO, logfile='log.txt'):
        """
        description:
        param:
        return:
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(log_level)

        self.console_handler_formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
            datefmt=None,
            reset=True,
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red',
            },
            secondary_log_colors={},
            style='%')

        self.console_handler = logging.StreamHandler()
        self.console_handler.setFormatter(self.console_handler_formatter)
        self.logger.addHandler(self.console_handler)

        self.file_handler_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')

        self.file_handler = logging.FileHandler(logfile)
        self.file_handler.setFormatter(self.file_handler_formatter)
        self.logger.addHandler(self.file_handler)

    def log(self, level, message):
        """
        description:
        param:
        return:
        """
        self.logger.log(level, message)

    def info(self, message):
        """
        description:
        param:
        return:
        """
        self.logger.info(message)

    def warning(self, message):
        """
        description:
        param:
        return:
        """
        self.logger.warning(message)

    def error(self, message):
        """
        description:
        param:
        return:
        """
        self.logger.error(message)

    def critical(self, message):
        """
        description:
        param:
        return:
        """
        self.logger.critical(message)

    def debug(self, message):
        """
        description:
        param:
        return:
        """
        self.logger.debug(message)



if __name__ == '__main__':
    logger = Logger()

    logger.info('this is a log message')
    logger.warning('this is a warning message')
    logger.error('this is an error message')
    logger.debug('this is a debug message')
    Logger().log(logging.INFO, 'this is a log message')
    print(f"They are same instance? {id(logger) == id(Logger())}")
