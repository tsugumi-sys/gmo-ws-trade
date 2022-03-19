import logging
import logging.handlers
import multiprocessing
from typing import Callable, Tuple

# References: https://docs.python.org/ja/3/howto/logging-cookbook.html
# Functions to enable logging in multiprocessing.


LOGGER_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s (%(filename)s:%(lineno)d)"


def listener_configurer():
    root = logging.getLogger()
    h = logging.handlers.RotatingFileHandler("./log/subprocesses.log", "a")
    # h = logging.StreamHandler()
    f = logging.Formatter(LOGGER_FORMAT)
    h.setFormatter(f)
    root.addHandler(h)


def listener_process(queue: multiprocessing.Queue, configurer: Callable):
    configurer()
    while True:
        try:
            record = queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            import sys
            import traceback

            print("Whoops! Problem:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


def worker_configurer(queue: multiprocessing.Queue, level: Tuple[str, int]):
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(level)
