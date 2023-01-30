from loguru import logger


def debug_format():
    # Custom debug level formatter, saves debug mssgs to a log file
    logger.add(
        "_logs/debug_{time:YY-MM-DD-HH-mm-ss!UTC}.log",
        level="DEBUG",
        format="Custom log: {level} | {time:HH:mm:ss!UTC} | {module}.{function} | {message}",
        rotation="10 KB",
        retention="10 seconds",
        colorize=True
    )


# For direct script execution:
if __name__ == '__main__':
    # More to dunder name variable __name__: https://www.pythontutorial.net/python-basics/python-__name__/
    debug_format()

