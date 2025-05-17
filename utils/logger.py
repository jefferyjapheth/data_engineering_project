import os
import logging

def get_project_data_dir():
    """
    Returns the absolute path to the project's data directory.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(script_dir, "..", "data"))
    return data_dir

def setup_logger(name: str, level=logging.INFO, log_file: str = None):
    """
    Sets up a logger that logs only to a file.

    Args:
        name (str): The name of the logger.
        level (int): Logging level.
        log_file (str): Optional custom log file path.

    Returns:
        logging.Logger: Configured logger instance.
    """
    if log_file is None:
        data_dir = get_project_data_dir()
        log_file = os.path.join(data_dir, "logs", "etl.log")

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
