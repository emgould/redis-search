"""
Custom logging setup for Firebase Cloud Functions.

This module configures the Python logging system to output via print()
which is automatically captured by Cloud Logging in deployed Cloud Functions.

Import this at the top of main.py to configure logging for all modules.
"""

import logging
import os
import sys


class CloudLoggingHandler(logging.Handler):
    """
    Custom logging handler that outputs to stdout via print().
    Cloud Functions automatically captures stdout and sends it to Cloud Logging.
    """

    def emit(self, record):
        try:
            msg = self.format(record)
            # Use print() which is captured by Cloud Logging
            print(msg, file=sys.stdout)
        except Exception:
            self.handleError(record)


class CloudLoggingFormatter(logging.Formatter):
    """
    Formatter that includes log level prefix for better visibility.
    """

    def format(self, record):
        # Add level prefix similar to cloud_log
        level_prefix = record.levelname
        message = super().format(record)
        return f"{level_prefix}: {message}"


def setup_cloud_logging():
    """
    Configure the root logger to use CloudLoggingHandler.
    This makes all logger.info(), logger.error(), etc. calls work in Cloud Functions.

    Call this once at the start of your application (in main.py).
    """
    # Detect if running in emulator vs deployed Cloud Functions
    is_emulator = bool(
        os.getenv('FIRESTORE_EMULATOR_HOST') or
        os.getenv('FIREBASE_AUTH_EMULATOR_HOST') or
        os.getenv('FUNCTIONS_EMULATOR')
    )

    # Get the root logger
    root_logger = logging.getLogger()

    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    if is_emulator:
        # In emulator, use standard logging with simple format
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(levelname)-8s %(name)s: %(message)s')
    else:
        # In deployed Cloud Functions, use custom handler with print()
        handler = CloudLoggingHandler()
        formatter = CloudLoggingFormatter('%(message)s')

    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    # Log that setup is complete
    if is_emulator:
        root_logger.info("📋 Logging configured for emulator (standard logging)")
    else:
        root_logger.info("☁️  Logging configured for Cloud Functions (print-based)")

