"""
DataFrame-aware logging utilities.
"""

from __future__ import annotations

import sys
import json
from datetime import datetime
from typing import Literal
import pandas as pd

LEVELS: dict[str, int] = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
}

LogMode = Literal["text", "json"]

class DFLogger:
    """
    DataFrame-aware file logger for pipeline execution.

    Parameters
    ----------
    log_file : str, default="dfflow.log"
        Output log file path.
    min_level : str, default="INFO"
        Minimum log level to record.
    max_rows : int | None, default=20
        Max DataFrame rows to include in log preview.
    max_cols : int | None, default=20
        Max DataFrame columns to include in log preview.
    mode : {"text", "json"}, default="text"
        Log format mode.
    file_mode : str, default="w"
        File open mode. "w" = overwrite, "a" = append.
    """

    TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        log_file: str = "dfflow.log",
        min_level: str = "INFO",
        max_rows: int | None = 20,
        max_cols: int | None = 20,
        mode: LogMode = "text",
        file_mode: str = "w",
    ) -> None:

        if min_level not in LEVELS:
            raise ValueError(f"Invalid log level: '{min_level}'. "
                             f" Must be one of: {list(LEVELS.keys())}")

        if mode not in ("text", "json"):
            raise ValueError(f"Invalid mode: '{mode}'. Must be 'text' or 'json'.")

        if file_mode not in ("w", "a"):
            raise ValueError(f"Invalid file mode: '{file_mode}'. Must be 'w' or 'a'.")

        self.log_file = log_file
        self.min_level = min_level
        self.max_rows = max_rows
        self.max_cols = max_cols
        self.mode = mode
        self.file_mode = file_mode
        self._initialized = False

    def debug(self, message: str, df: pd.DataFrame | None = None) -> None:
        """
        Log a DEBUG-level message.

        Intended for detailed diagnostic information.
        """
        self._log("DEBUG", message, df)

    def info(self, message: str, df: pd.DataFrame | None = None) -> None:
        """
        Log an INFO-level message.

        Used for standard pipeline progress.
        """
        self._log("INFO", message, df)

    def warning(self, message: str, df: pd.DataFrame | None = None) -> None:
        """
        Log a WARNING-level message.

        Used to indicate potential issues such as data loss
        or unexpected DataFrame changes.
        """
        self._log("WARNING", message, df)

    def error(self, message: str, df: pd.DataFrame | None = None) -> None:
        """
        Log an ERROR-level message.

        Used to report critical failures.
        """
        self._log("ERROR", message, df)

    def _log(self, level: str, message: str, df: pd.DataFrame | None) -> None:
        """
        Internal logging implementation.

        Handles log-level filtering and writing output.
        """
        if LEVELS[level] < LEVELS[self.min_level]:
            return

        if df is not None and not isinstance(df, pd.DataFrame):
            raise TypeError("df must be pandas DataFrame or None")

        timestamp = datetime.now().strftime(self.TIMESTAMP_FMT)

        entry = (
            self._format_json(timestamp, level, message, df)
            if self.mode == "json"
            else self._format_text(timestamp, level, message, df)
        )

        actual_mode = self.file_mode if not self._initialized else "a"
        self._initialized = True

        try:
            with open(self.log_file, actual_mode, encoding="utf-8") as f:
                f.write(entry)

        except (IOError, OSError) as e:
            print(f"[DFLogger] Failed due to write log files: {e}", file=sys.stderr)

    def _format_text(
            self,
            timestamp: str,
            level: str,
            message: str,
            df: pd.DataFrame | None,
    ) -> str:
        """
        Build human-readable text log entry.
        """
        lines = [f"{timestamp} | {level}", message]

        if df is not None:
            lines.append(f"Shape: {df.shape}")
            preview = df.head(self.max_rows).iloc[:, : self.max_cols]
            lines.append(preview.to_string())

        lines.append("")
        return "\n".join(lines) + "\n"

    def _format_json(
            self,
            timestamp: str,
            level: str,
            message: str,
            df: pd.DataFrame | None,
    ) -> str:
        """
        Build structured JSON log entry (newline-delimited).
        """
        entry: dict[str, object] = {
            "timestamp": timestamp,
            "level": level,
            "message": message,
        }

        if df is not None:
            preview = df.head(self.max_rows).iloc[:, : self.max_cols]
            entry["shape"] = list(df.shape)
            entry["columns"] = df.columns.tolist()
            entry["preview"] = preview.to_dict(orient="list")

        return json.dumps(entry, default=str) + "\n"
