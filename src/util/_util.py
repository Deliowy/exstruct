import logging
import os
from logging import Logger
import keyword

LOG_MESSAGE_FORMAT = "%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
LOG_DATETIME_FORMAT = "%d-%b-%y %H:%M:%S"


def getLogger(logger_name: str, logs_folder: str = None) -> Logger:
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
        result = string.translate(str.maketrans(" .,-/\\", "______"))

    if (
        keyword.iskeyword(result)
        or keyword.issoftkeyword(result)
        or string[0].isdigit()
    ):
        result = f"_{result}"

    return result


def normalize_str(string: str):
    result = string
    if string:
        # Add space before and in-between triple-quote in string to prevent false triple-quote termination
        result = ' "" "'.join(result.rsplit('"""'))

    return result
