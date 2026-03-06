import time
from contextlib import contextmanager
from collections.abc import Iterator


@contextmanager
def timer() -> Iterator[callable[[], int]]:
    start = time.perf_counter()

    def elapsed_ms() -> int:
        return int((time.perf_counter() - start) * 1000)

    yield elapsed_ms
