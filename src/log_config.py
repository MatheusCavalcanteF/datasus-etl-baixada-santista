"""Configuração de logging compartilhada pelas etapas do pipeline."""

import logging
import sys

from src.config import LOG_DIR

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def setup_logging(level: int = logging.INFO, log_file: str = "pipeline.log") -> None:
    """Envia os logs para o console e para logs/<log_file> (UTF-8, modo append)."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / log_file, encoding="utf-8"),
    ]
    logging.basicConfig(level=level, format=LOG_FORMAT, handlers=handlers, force=True)
