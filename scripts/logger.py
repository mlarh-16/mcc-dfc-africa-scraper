"""
scripts/logger.py
─────────────────
Shared logger for the MCC Africa scraper.
Writes to both console and output/run_log.txt simultaneously.
"""

import logging
import sys
from scripts.config import FILES


def get_logger(name: str = "mcc_scraper") -> logging.Logger:
    """
    Returns a configured logger that writes to:
      - stdout (console)
      - output/run_log.txt (file)
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured — don't add duplicate handlers

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    # File handler
    file_h = logging.FileHandler(FILES["run_log"], mode="a", encoding="utf-8")
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(fmt)

    logger.addHandler(console)
    logger.addHandler(file_h)

    return logger
