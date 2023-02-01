import logging

from loguru import logger


def trace_format():
    logger.add(
        # See: https://loguru.readthedocs.io/en/stable/api/logger.html#file
        logging.StreamHandler(),
        level="INFO",  # Python's logging doesn't have the severity level TRACE!
        # for loguru severity levels rankings see the link above (trace -> debug -> info -> etc),
        # for Python's logging severity levels see https://docs.python.org/3/howto/logging.html#when-to-use-logging
        format="{level} | {time:HH:mm:ss!UTC} | {module}.{function} | {message}",
        colorize=True
    )
