import logging
import os

def setup_logger(name: str, level=logging.INFO, log_file: str = "logs/streaming.log"):
    """
    Sets up a logger that logs to both console and a file.

    Args:
        name (str): The name of the logger.
        level (int): Logging level.
        log_file (str): File path to log to (default: logs/streaming.log).

    Returns:
        logging.Logger: Configured logger.
    """
    # Ensure log directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Define format
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
