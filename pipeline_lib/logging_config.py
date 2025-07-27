import pipeline_lib.config as cfg
import logging
import os

LOG_DIR = cfg.DATA_LOG_DIR_PATH
LOG_LEVEL = logging.DEBUG


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)

    if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith("pipeline.log")
               for h in root_logger.handlers):
        general_handler = logging.FileHandler(os.path.join(LOG_DIR, "pipeline.log"))
        general_handler.setFormatter(formatter)
        root_logger.addHandler(general_handler)

    for component in ['fetch', 'transform', 'olap', 'transform_modules', 'utils']:
        logger = logging.getLogger(f'pipeline.{component}')
        logger.setLevel(LOG_LEVEL)
        logger.propagate = False

        if not logger.handlers:
            handler = logging.FileHandler(os.path.join(LOG_DIR, f"{component}.log"))
            handler.setFormatter(formatter)
            logger.addHandler(handler)
