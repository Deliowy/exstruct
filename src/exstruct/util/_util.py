import logging
import logging.handlers
import os
from logging import Logger
import keyword

LOG_MESSAGE_FORMAT = "%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
LOG_DATETIME_FORMAT = "%d-%b-%y %H:%M:%S"


def getLogger(logger_name: str, logs_folder: str = None) -> Logger:
    """Make logger with rotating file handler

    Args:
        logger_name (str): name for logger
        logs_folder (str, optional): folder where log files will be. Defaults to None.

    Returns:
        Logger: logger with rotating file handler
    """    
    if not logs_folder:
        logs_folder = ".logs"
    os.makedirs(".logs", exist_ok=True)
    logging.basicConfig(
        encoding="utf-8",
        level=logging.INFO,
        format=LOG_MESSAGE_FORMAT,
        datefmt=LOG_DATETIME_FORMAT,
        force=True,
    )
    logger = logging.getLogger(f"{logger_name}")
    logger.setLevel(logging.INFO)
    logger_formatter = logging.Formatter(
        fmt=LOG_MESSAGE_FORMAT,
        datefmt=LOG_DATETIME_FORMAT,
    )
    logger_handler = logging.handlers.RotatingFileHandler(
        f".logs/{logger_name}.log",
        maxBytes=1024 * 1024 * 250,
        backupCount=10,
        encoding="utf-8",
    )
    logger_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_handler)
    return logger


def to_var_name(string: str):
    """Translate given string to viable python variable name

    Args:
        string (str):

    Returns:
        str: string, meeting python variables naming rules
    """
    result = string
    if string:
        result = string.translate(str.maketrans("@$ .,-/\\", "________"))

    if (
        keyword.iskeyword(result)
        or keyword.issoftkeyword(result)
        or string[0].isdigit()
    ):
        result = f"_{result}"

    return result

# TODO Improve naming and possible use-cases
def normalize_str(string: str):
    """Normalize string with multiple quotes in it

    Args:
        string (str): string with quotes

    Returns:
        str: normalized string
    """    
    result = string
    if string:
        # Add space before and in-between triple-quote in string to prevent false triple-quote termination
        result = ' "" "'.join(result.rsplit('"""'))

    return result
