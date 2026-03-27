"""Asynchronous utilities"""

import warnings
from collections import abc
from concurrent import futures
from traceback import format_exception
from concurrent.futures import Future, ThreadPoolExecutor


__all__ = (
    "background",
    "queue",
    "merge",
    "set_running",
    "set_result",
    "set_exception",
    "warn_exception"
)


def queue(name: str = ""):
    """Background single-thread task queue"""
    return ThreadPoolExecutor(max_workers=1, thread_name_prefix=name)


def background(func: abc.Callable, /, *args, **kwds):
    """Background function as future"""
    pool = queue(name=func.__name__)
    future = pool.submit(func, *args, **kwds)
    pool.shutdown(wait=False)
    return future


def disable_logger():
    """Disable future logger"""
    import sys
    import logging
    logger = logging.getLogger("concurrent.futures")
    logger.setLevel(sys.maxsize)


def set_running(future: Future) -> bool:
    """Set future running (if plausible)"""
    try:
        return future.set_running_or_notify_cancel()
    except RuntimeError:
        return False


def set_result(future: Future, result) -> bool:
    """Set future result (if plausible)"""
    try:
        future.set_result(result)
    except futures.InvalidStateError:
        return False
    else:
        return True


def set_exception(future: Future, exc: BaseException) -> bool:
    """Set future exception (if plausible)"""
    try:
        future.set_exception(exc)
    except futures.InvalidStateError:
        return False
    else:
        return True


def warn_exception[T](future: Future[T]) -> None:
    """Future handler that warns about exceptions"""
    if (exc := future.exception()) is not None:
        warnings.warn("".join(format_exception(exc)), RuntimeWarning)


def merge(*fs: Future, return_when=futures.ALL_COMPLETED) -> Future:
    """
    Combines multiple futures

    FIRST_COMPLETED: when any future finishes or is cancelled, returns its result
    FIRST_EXCEPTION: when any future finishes by raising an exception, raises its exception
    ALL_COMPLETED: when all futures finish or are cancelled, returns None

    When no futures are provided, returns None
    """
    future = Future()
    fs = frozenset(fs)  # type: ignore
    done = set()

    # Callbacks
    def handle_done(future):
        done.add(future)

        # Single case
        try:
            result = future.result()
        except Exception as exc:
            if return_when == futures.FIRST_COMPLETED or return_when == futures.FIRST_EXCEPTION:
                set_exception(future, exc)
        else:
            if return_when == futures.FIRST_COMPLETED:
                set_result(future, result)

        # Multi case
        if len(done) >= len(fs):
            set_result(future, None)

    future.futures = fs  # type: ignore
    set_running(future)

    for future in fs:
        future.add_done_callback(handle_done)

    # Empty case
    if len(fs) <= 0:
        set_result(future, None)

    return future


disable_logger()
