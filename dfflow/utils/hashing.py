from datetime import datetime


def current_timestamp() -> str:
    """
    Get the current timestamp as a formatted string.

    Returns the current date and time in the format
    ``YYYY-MM-DD HH:MM:SS``.

    Returns
    -------
    str
        Current timestamp as a formatted string.
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
