"""
Centralized logging configuration.

Uses stdout so logs are captured by AWS Glue/EMR CloudWatch integration
without any additional setup. Format is structured for easy log parsing.
"""

import logging
import sys


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Create a production-ready logger.

    Args:
        name:  Logger name — pass __name__ from the calling module.
        level: Log level string (DEBUG | INFO | WARNING | ERROR).

    Returns:
        Configured Logger instance. Idempotent: calling twice for the
        same name returns the same logger without duplicate handlers.
    """
    logger = logging.getLogger(name)

    # Guard against duplicate handlers when module is re-imported
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger