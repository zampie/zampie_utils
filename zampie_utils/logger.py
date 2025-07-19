import logging
from rich.logging import RichHandler
from .singleton import Singleton


class Logger(metaclass=Singleton):
    """"""

    def __init__(
        self, log_level=logging.INFO, logfile="log.txt", enable_file_handler=False
    ):
        """
        description: 初始化日志记录器
        param:
            log_level: 日志级别
            logfile: 日志文件路径
            enable_file_handler: 是否启用文件处理器
        return:
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(log_level)

        # 控制台处理器 - 使用 Rich
        self.console_handler = RichHandler()
        # Rich 自带格式化，无需额外设置 formatter
        self.logger.addHandler(self.console_handler)

        # 文件处理器
        self.file_handler = None
        if enable_file_handler:
            self.enable_file_handler(logfile)

        # 直接指向方法，避免间接调用，无法定位到文件
        self.info = self.logger.info
        self.warning = self.logger.warning
        self.error = self.logger.error
        self.critical = self.logger.critical
        self.debug = self.logger.debug

    def enable_file_handler(self, logfile="log.txt"):
        """
        description: 启用文件处理器
        param:
            logfile: 日志文件路径
        return:
        """
        if self.file_handler is None:
            self.file_handler_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            self.file_handler = logging.FileHandler(logfile)
            self.file_handler.setFormatter(self.file_handler_formatter)
            self.logger.addHandler(self.file_handler)

    def disable_file_handler(self):
        """
        description: 禁用文件处理器
        param:
        return:
        """
        if self.file_handler is not None:
            self.logger.removeHandler(self.file_handler)
            self.file_handler = None

    def set_log_level(self, log_level: str):
        """
        description: 设置日志级别
        param:
            log_level: 日志级别
        return:
        """
        self.logger.setLevel(log_level)

if __name__ == "__main__":
    # 默认不启用文件处理器
    logger = Logger()
    logger.info("this is a log message")
    logger.warning("this is a warning message")
    logger.error("this is an error message")
    logger.debug("this is a debug message")

    # 启用文件处理器
    logger.enable_file_handler("test.log")
    logger.info("this will be written to file")

    # 禁用文件处理器
    logger.disable_file_handler()
    logger.info("this will not be written to file")

    print(f"They are same instance? {id(logger) == id(Logger())}")
