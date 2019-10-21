import sys
import logging


def logger(name, level=logging.WARNING):

    # create module logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        # create console handler with a higher log level
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(level)
        # add the handlers to the logger
        logger.addHandler(ch)

    return logger


class LoggerMixin:

    def __init__(self, level=logging.WARNING, *args, **kwargs):
        self._level = level
        super().__init__(*args, **kwargs)

    @property
    def log(self):
        if not hasattr(self, '_logger'):
            name = '.'.join([self.__module__, self.__class__.__name__])
            _logger = logger(name, self._level)
            setattr(self, '_logger', _logger)
        return self._logger

    def set_formatter(self, fmt: str) -> None:
        formatter = logging.Formatter(fmt)
        for handler in self.log.handlers:
            handler.setFormatter(formatter)

    def set_level(self, level: int) -> None:
        for handler in self.log.handlers:
            handler.setLevel(level)
        self.log.setLevel(level)
