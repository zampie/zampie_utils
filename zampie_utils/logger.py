import logging
from logging.handlers import RotatingFileHandler
from rich.logging import RichHandler
from .singleton import Singleton

# 添加自定义日志级别 NOTICE (25) - 位于 INFO (20) 和 WARNING (30) 之间
NOTICE_LEVEL = 25
logging.addLevelName(NOTICE_LEVEL, "NOTICE")


class Logger(metaclass=Singleton):
    """"""

    def __init__(self):
        """
        description: 初始化日志记录器
        param:
            log_level: 日志级别
            logfile: 日志文件路径
            enable_file_handler: 是否启用文件处理器
        return:
        """
        self.logger = logging.getLogger("zampie_utils.logger")
        self.logger.setLevel(logging.INFO)

        # 控制台处理器 - 使用 Rich
        self.console_handler = RichHandler()
        # Rich 自带格式化，无需额外设置 formatter
        self.logger.addHandler(self.console_handler)

        # 直接指向方法，避免间接调用，无法定位到文件
        self.none = lambda *args, **kwargs: None
        self.debug = self.logger.debug
        self.info = self.logger.info
        self.notice = self._notice  # 自定义notice方法
        self.warning = self.logger.warning
        self.error = self.logger.error
        self.critical = self.logger.critical

        self.log_router = {
            "none": self.none,
            "debug": self.debug,
            "info": self.info,
            "notice": self.notice,
            "warning": self.warning,
            "error": self.error,
            "critical": self.critical,
        }

        self.file_handler_dict = {}
        self.file_handler_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

    def _notice(self, message, *args, **kwargs):
        """
        description: 自定义notice级别日志方法
        param:
            message: 日志消息
            *args: 额外参数
            **kwargs: 额外关键字参数
        return:
        """
        if self.logger.isEnabledFor(NOTICE_LEVEL):
            self.logger._log(NOTICE_LEVEL, message, args, **kwargs)

    def log(self, level: str, message, *args, **kwargs):
        """
        description: 自定义日志方法
        param:
            level: 日志级别
            message: 日志消息
        """

        return self.log_router.get(level.lower(), self.info)(message, *args, **kwargs)

    def add_console_handler(self):
        """
        description: 添加控制台处理器
        param:
        return:
            bool: 是否成功添加控制台处理器
        """
        # 检查控制台处理器是否已经存在
        if self.console_handler in self.logger.handlers:
            self.logger.warning("控制台处理器已经存在，跳过添加")
            return False

        try:
            self.logger.addHandler(self.console_handler)
            self.logger.info("成功添加控制台处理器")
            return True
        except Exception as e:
            self.logger.error(f"添加控制台处理器失败: {str(e)}")
            return False

    def remove_console_handler(self):
        """
        description: 移除控制台处理器
        param:
        return:
            bool: 是否成功移除控制台处理器
        """
        if self.console_handler is None:
            self.logger.warning("控制台处理器不存在，无法移除")
            return False

        try:
            self.logger.removeHandler(self.console_handler)
            self.console_handler = None
            return True
        except Exception as e:
            self.logger.error(f"移除控制台处理器失败: {str(e)}")
            return False

    def add_file_handler(
        self,
        logfile="log.txt",
        log_level=None,
        max_bytes=10 * 1024 * 1024,
        backup_count=5,
    ):
        """
        description: 启用文件处理器，支持自动滚动日志
        param:
            logfile: 日志文件路径
            log_level: 文件处理器的日志级别，默认使用全局日志级别
            max_bytes: 单个日志文件的最大大小（字节），默认10MB
            backup_count: 保留的备份文件数量，默认5个
        return:
            bool: 是否成功添加文件处理器
        """
        try:
            # 检查是否已经存在该文件的处理器
            if logfile in self.file_handler_dict:
                self.logger.warning(f"文件处理器 {logfile} 已经存在，跳过添加")
                return False

            file_handler = RotatingFileHandler(
                logfile, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
            )

            # 设置日志级别
            if log_level is not None:
                file_handler.setLevel(log_level)

            file_handler.setFormatter(self.file_handler_formatter)
            self.logger.addHandler(file_handler)
            self.file_handler_dict[logfile] = file_handler

            self.logger.info(
                f"成功添加文件处理器: {logfile} (最大大小: {max_bytes / 1024 / 1024:.1f}MB, 备份数量: {backup_count})"
            )
            return True

        except Exception as e:
            self.logger.error(f"添加文件处理器失败 {logfile}: {str(e)}")
            return False

    def remove_file_handler(self, logfile="log.txt"):
        """
        description: 移除文件处理器
        param:
            logfile: 日志文件路径
        return:
            bool: 是否成功移除文件处理器
        """
        if logfile in self.file_handler_dict:
            try:
                self.logger.removeHandler(self.file_handler_dict[logfile])
                self.file_handler_dict.pop(logfile)
                self.logger.info(f"成功移除文件处理器: {logfile}")
                return True
            except Exception as e:
                self.logger.error(f"移除文件处理器失败 {logfile}: {str(e)}")
                return False
        else:
            self.logger.warning(f"文件处理器 {logfile} 不存在")
            return False

    def set_level(self, log_level: str):
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
    logger.notice("this is a notice message")  # 新增的notice级别
    logger.warning("this is a warning message")
    logger.error("this is an error message")
    logger.debug("this is a debug message")

    # 启用文件处理器，使用默认滚动设置（10MB，5个备份）
    logger.add_file_handler("test.log")
    logger.info("this will be written to file")
    logger.notice("this notice will also be written to file")

    # 启用文件处理器，自定义滚动设置（5MB，3个备份）
    logger.add_file_handler("custom.log", max_bytes=5 * 1024 * 1024, backup_count=3)
    logger.info("this will be written to custom file with custom rotation settings")

    # 禁用文件处理器
    logger.remove_file_handler("test.log")
    logger.remove_file_handler("custom.log")
    logger.info("this will not be written to file")

    # 这些调用都不会输出任何内容
    logger.none("这条消息不会显示")
    logger.log("none", "这条消息也不会显示")

    # 其他级别的日志正常输出
    logger.info("这条消息会显示")
    logger.error("这条错误消息会显示")

    print(f"They are same instance? {id(logger) == id(Logger())}")
