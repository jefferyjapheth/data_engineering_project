
import logging

def setup_logger(name: str, level=logging.INFO):
    """
    Sets up a logger with the specified name and logging level.

    Args:
        name (str): The name of the logger (typically the module or script name).
        level (int): The logging level (default is INFO).
    
    Returns:
        logging.Logger: The configured logger instance.
    """
    # Set up logging format
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=level, format=log_format)
    
    # Create logger instance
    logger = logging.getLogger(name)
    
    return logger
