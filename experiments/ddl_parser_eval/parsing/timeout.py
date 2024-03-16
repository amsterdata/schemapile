# https://stackoverflow.com/questions/492519/timeout-on-a-function-call

from contextlib import contextmanager
import signal
import time

@contextmanager
def timeout(duration):
    def timeout_handler(signum, frame):
        raise TimeoutError('TIMEOUT ERROR')
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(duration)
    try:
        yield
    finally:
        signal.alarm(0)