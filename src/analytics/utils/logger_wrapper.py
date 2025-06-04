from datetime import datetime
from typing import Optional

import pytz


class LoggerWrapper:
    """
    LoggerWrapper is a singleton that wraps Log4jLogger and allows for logging messages
    with both print and Log4jLogger functionality. Each log message is printed with a timestamp
    and log level (INFO, ERROR, WARN).
    """
    _instance: Optional['LoggerWrapper'] = None

    def __new__(cls, logger=None) -> 'LoggerWrapper':
        """
        Ensures that only one instance of LoggerWrapper is created (singleton pattern).

        Args:
            logger (Log4jLogger): The instance of the Log4jLogger class.

        Returns:
            LoggerWrapper: The singleton instance of LoggerWrapper.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.logger = logger  # Initialize the Log4jLogger instance only once
        return cls._instance

    @staticmethod
    def _get_timestamp() -> str:
        """
        Retrieves the current timestamp in 'YYYY-MM-DD HH:MM:SS' format.

        Returns:
            str: The current timestamp as a string.
        """
        timezone = pytz.timezone('Etc/GMT+5')
        current_time = datetime.now(tz=timezone)
        return current_time.strftime('%Y-%m-%d %H:%M:%S')

    def _format_message(self, level: str, msg: str) -> str:
        """
        Formats a log message by prepending the current timestamp and log level.

        Args:
            level (str): The log level (e.g., "INFO", "ERROR", "WARN").
            msg (str): The log message.

        Returns:
            str: The formatted log message with timestamp and level tag.
        """
        timestamp = self._get_timestamp()
        return f"[{timestamp}] [{level}] {msg}"

    def info(self, msg: str) -> None:
        """
        Logs an informational message to both console (with a timestamp and level) and Log4jLogger.

        Args:
            msg (str): The informational message to log.
        """
        formatted_msg = self._format_message("INFO", msg)
        print(formatted_msg)
        self.logger.info(msg)

    def error(self, msg: str) -> None:
        """
        Logs an error message to both console (with a timestamp and level) and Log4jLogger.

        Args:
            msg (str): The error message to log.
        """
        formatted_msg = self._format_message("ERROR", msg)
        print(formatted_msg)
        self.logger.error(msg)

    def warn(self, msg: str) -> None:
        """
        Logs a warning message to both console (with a timestamp and level) and Log4jLogger.

        Args:
            msg (str): The warning message to log.
        """
        formatted_msg = self._format_message("WARN", msg)
        print(formatted_msg)
        self.logger.warn(msg)
