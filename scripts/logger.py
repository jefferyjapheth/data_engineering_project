import logging
import os

# Ensure the log directory exists
LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, 'pipeline.log')

def get_logger(name: str) -> logging.Logger:
    """
    Create or retrieve a logger with the given name.
    Logs to both a central file and the console.
    Ensures no duplicate handlers are added.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent log duplication in root logger

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
