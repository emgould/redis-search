import logging
import os
import warnings
from datetime import UTC, datetime
from logging.handlers import TimedRotatingFileHandler

import pytz

# Suppress specific deprecation warnings from Lark
warnings.filterwarnings("ignore", category=DeprecationWarning, module="lark.utils")

# Setup Environment
TIMEZONE = pytz.timezone("America/New_York")

"""
Multi-logger setup
logs to console and to /var/log
"""

Logger_Cache: dict[str, logging.Logger] = {}
Default_Level = logging.INFO


def set_level(level):
    global Default_Level
    Default_Level = level


class LocalTimeFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.est_tz = TIMEZONE

    def format(self, record):
        utc_dt = datetime.fromtimestamp(record.created, UTC).replace(
            tzinfo=pytz.utc
        )
        # Convert the time to EST
        est_time = utc_dt.astimezone(self.est_tz)

        # Format the time in 12-hour format
        record.est_time = est_time.strftime("%I:%M:%S %p")
        record.name = record.name[0:20]
        if record.levelno == logging.WARN:
            self._style._fmt = "%(est_time)-10s %(name)-20s:%(levelname)-8s =====> Warning %(message)s\n"

        elif record.levelno == logging.ERROR:
            self._style._fmt = "\n\n%(est_time)-10s %(name)-20s =====> ERROR \n%(message)s\n---END ERROR ---\n"

        else:
            # Set fixed widths for different parts of the log message
            self._style._fmt = "%(est_time)-10s %(name)-20s:%(levelname)-8s %(message)s"

        return super().format(record)


class LocalFileFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.utc_tz = pytz.UTC  # Add UTC timezone instance

    def format(self, record):
        utc_dt = datetime.fromtimestamp(record.created, UTC).replace(
            tzinfo=pytz.utc
        )
        # Use the UTC timezone instance
        local_dt = utc_dt.astimezone(self.utc_tz)

        # Format the time manually
        record.local_time = local_dt.strftime("%Y-%m-%d %H:%M:%S")
        if record.levelno in (logging.WARN, logging.ERROR):
            self._style._fmt = "\n===== ERROR Source: %(name)s =====\n%(local_time)s:%(message)s\n---END ERROR ---\n"
        else:
            self._style._fmt = "%(local_time)s:%(name)15s:%(levelname)s %(message)s"

        return super().format(record)


def get_logger(name: str, level=None, filename=None, app_name=None) -> logging.Logger:
    """Return a logger with the specified name."""
    if name in Logger_Cache:
        return Logger_Cache[name]

    if level is None:
        level = Default_Level
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Add debug print to verify logger setup
    #print(f"Setting up logger '{name}' with level {logging.getLevelName(level)}")

    # Add console handler first
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = LocalTimeFormatter()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if filename:
        path = "/tmp/log/mediacircle"
        os.makedirs(path, exist_ok=True)
        fullpath = os.path.join(path, os.path.basename(filename))
        fh_formatter = LocalFileFormatter()
        try:
            fh = TimedRotatingFileHandler(fullpath, when="midnight", backupCount=30)
            fh.setLevel(level)
            fh.setFormatter(fh_formatter)
            logger.addHandler(fh)
        except FileNotFoundError:
            # If rotation fails, just create a regular FileHandler
            fh_fallback = logging.FileHandler(fullpath)
            fh_fallback.setLevel(level)
            fh_fallback.setFormatter(fh_formatter)
            logger.addHandler(fh_fallback)
    else:
        # Remove the console handler
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                logger.removeHandler(handler)

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)

        # Create and set formatter to console handler
        formatter = LocalTimeFormatter()
        ch.setFormatter(formatter)

        # Add console handler to logger
        logger.addHandler(ch)

    logger.propagate = False


    Logger_Cache[name] = logger

    return logger


if __name__ == "__main__":
    a = get_logger("test")
    a.info("this is a test")
    a.error("this is an error test")
