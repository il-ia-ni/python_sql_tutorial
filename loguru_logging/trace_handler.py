import logging

from loguru import logger


def trace_format():
    logger.add(
        # See: https://loguru.readthedocs.io/en/stable/api/logger.html#file
        logging.StreamHandler(),
        level="TRACE",  # for severity levels rankings see the link above (trace -> debug -> info -> etc)
        format="{level} | {time:HH:mm:ss!UTC} | {module}.{function} | {message}",
        colorize=True
    )
