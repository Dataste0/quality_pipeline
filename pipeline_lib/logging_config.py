import pipeline_lib.config as cfg
import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = cfg.DATA_LOG_DIR_PATH
LOG_LEVEL = logging.INFO

# Parametri rotazione
LOG_FILENAME = "pipeline.log"
MAX_BYTES = 10 * 1024 * 1024  # 10 MB prima della rotazione
BACKUP_COUNT = 5  # conserva pipeline.log.1 ... pipeline.log.5

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)

    log_path = os.path.join(LOG_DIR, LOG_FILENAME)
    log_format = "%(asctime)s [%(levelname)s] (%(filename)s/%(funcName)s) - %(message)s"
    formatter = logging.Formatter(log_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)

    # aggiunge un RotatingFileHandler se non esiste già per quel file
    if not any(
        isinstance(h, RotatingFileHandler) and os.path.abspath(h.baseFilename) == os.path.abspath(log_path)
        for h in root_logger.handlers
    ):
        handler = RotatingFileHandler(
            log_path,
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
            encoding="utf-8"
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
