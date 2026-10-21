import logging
import sys


def configure_logging(level: int = logging.INFO):
    """Configure structured logging format for production readiness."""
    log_format = "%(asctime)s.%(msecs)03d %(levelname)-8s [%(name)s] %(message)s"
    date_format = "%Y-%m-%dT%H:%M:%S"

    logging.basicConfig(
        level=level,
        format=log_format,
        datefmt=date_format,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )