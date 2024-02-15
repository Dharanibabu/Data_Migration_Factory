import logging

from colorlog import ColoredFormatter


class LoggerUtils:
    def setup_logger(self):
        logger = logging.getLogger("Logger")  # Create a logger instance
        logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()  # Logs to the console
        formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(name)s-%(filename)s:%(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            reset=True,
            log_colors={
                "DEBUG": "blue",
                "INFO": "white",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "yellow",
            },
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger
