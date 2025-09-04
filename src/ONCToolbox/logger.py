import logging
import time

def setup_logger(logger_name: str,
                 level: int = 10) -> object:

    """
    Set up a logger object for displaying verbose messages to console.

    :param logger_name: The unique logger name to use. Can be shared between modules
    :param level: The logging level to use. Default is 10 which corresponds to logging.DEBUG.
    :return: The configured logging.Logger object.
    """

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    if logger.hasHandlers():
        logger.handlers.clear()

    console = logging.StreamHandler()
    console.setLevel(level)

    # Set the logging format.
    dtfmt = '%Y-%m-%dT%H:%M:%S'
    strfmt = f'%(asctime)s.%(msecs)03dZ | %(name)-12s | %(levelname)-8s | %(message)s'
    fmt = logging.Formatter(strfmt, datefmt=dtfmt)
    fmt.converter = time.gmtime
    console.setFormatter(fmt)
    logger.addHandler(console)
    logger.propagate = False
    return logger
