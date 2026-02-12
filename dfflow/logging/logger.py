import json
from datetime import datetime
from typing import Literal
import pandas as pd


class DFLogger:
    """
    DataFrame-aware logger with log levels and optional JSON output.
    """
    TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"

    LEVELS = {
        "DEBUG": 10,
        "INFO": 20,
        "WARNING": 30,
        "ERROR": 40,
    }

    def __init__(
        self,
        log_file: str = "dfflow.log",
        mode: Literal["text", "json"] = "text",
        min_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
    ):
        """
        Initialize the DataFrame logger.

        Parameters
        ----------
        log_file : str
            Path to the log file.
        mode : {"text", "json"}
            Logging format to use.
        min_level : {"DEBUG", "INFO", "WARNING", "ERROR"}
            Minimum log level to record.
        """
        if mode not in ("text", "json"):
            raise ValueError("mode must be 'text' or 'json'")

        if min_level not in self.LEVELS:
            raise ValueError(f"Invalid log level: {min_level}")

        self.log_file = log_file
        self.mode = mode
        self.min_level = min_level

    def debug(self, message: str, df: pd.DataFrame):
        """
        Log a DEBUG-level message.

        Intended for detailed diagnostic information.
        """
        self._log("DEBUG", message, df)

    def info(self, message: str, df: pd.DataFrame):
        """
        Log an INFO-level message.

        Used for standard pipeline progress and status updates.
        """
        self._log("INFO", message, df)

    def warning(self, message: str, df: pd.DataFrame):
        """
        Log a WARNING-level message.

        Used to indicate potential issues such as data loss
        or unexpected DataFrame changes.
        """
        self._log("WARNING", message, df)

    def error(self, message: str, df: pd.DataFrame):
        """
        Log an ERROR-level message.

        Used to report critical failures or invalid states.
        """
        self._log("ERROR", message, df)

    def _log(self, level: str, message: str, df: pd.DataFrame):
        """
        Internal logging implementation.

        Handles log-level filtering and writing output
        in either text or JSON format.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")

        if self.LEVELS[level] < self.LEVELS[self.min_level]:
            return

        timestamp = datetime.now().strftime(self.TIMESTAMP_FMT)

        if self.mode == "json":
            record = {
                "timestamp": timestamp,
                "level": level,
                "message": message,
                "shape": df.shape,
                "columns": list(df.columns),
            }
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        else:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write("=" * 80 + "\n")
                f.write(f"Timestamp : {timestamp}\n")
                f.write(f"Level     : {level}\n")
                f.write(f"Message   : {message}\n")
                f.write(f"Shape     : {df.shape}\n")
                f.write("DataFrame :\n")
                f.write(df.to_string())
                f.write("\n\n")