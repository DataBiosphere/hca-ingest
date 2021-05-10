import time
from typing import Any, Callable


class RetryException(Exception):
    """
    Represents a failure after some number of retries
    """
    pass


def is_truthy(result: Any) -> bool:
    """
    Helper that returns True if the provided result is defined, False if not
    """
    return bool(result)


def retry(target_fn: Callable[..., Any], max_wait_time_seconds: int,
          poll_interval_seconds: int, success_predicate: Callable[[Any], bool], *args: str) -> Any:
    """
    Retries the target function until max_wait_time is exceeded or success_predicate returns True
    :param max_wait_time_seconds:
    :param target_fn: function to retry
    :param max_wait_time: max wait time in seconds to retry the target function
    :param poll_interval_seconds:  wait time between retries in seconds
    :param success_predicate: function that will determine if the provided result indicates a successful retry
    :param args: args to be provided to the target function
    :return: result of the retry function
    """
    time_waited = 0
    while time_waited < max_wait_time_seconds:
        result = target_fn(*args)
        if success_predicate(result):
            return result

        time.sleep(poll_interval_seconds)
        time_waited += poll_interval_seconds

    raise RetryException("timed out")
