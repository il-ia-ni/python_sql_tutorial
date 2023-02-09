from loguru import logger


def debug_format():
    # Custom debug level formatter, saves debug mssgs to a log file
    logger.add(
        # See: https://loguru.readthedocs.io/en/stable/api/logger.html#file
        "_logs/debug_{time:YY-MM-DD-HH-mm-ss}.log",  # A sink in form of a path to a file: https://loguru.readthedocs.io/en/stable/api/logger.html#sink
        level="DEBUG",  # for severity levels rankings see the link above (trace -> debug -> info -> etc)
        filter=lambda record: record["level"].name == "DEBUG",  # Found @ https://github.com/Delgan/loguru/issues/46
        format="Custom log: {level} | {time:HH:mm:ss!UTC} | {module}.{function} | {message}",
        rotation="1 KB",
        retention="10 seconds",
        colorize=True
    )


# For direct script execution:
if __name__ == '__main__':
    # More to dunder name variable __name__: https://www.pythontutorial.net/python-basics/python-__name__/
    debug_format()

