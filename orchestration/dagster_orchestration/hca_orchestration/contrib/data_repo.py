import time
import logging
import data_repo_client


class DataRepoApiRetryException(Exception):
    """
    Represents a failure after some number of retries against the data repo API
    """
    pass


def retry_api_request(api_fn, max_wait_time: int, poll_interval_seconds: int, retriable_status_codes: set, *args):
    """
    "Polls" on the given API call; any status codes return in the retriable_status_codes will not result in
    immediate failure but instead cause a retry until max_wait_time is reached.
    :param api_fn: API method to call
    :param max_wait_time: Maximum wait time in seconds to poll
    :param poll_interval_seconds: Interval between polling calls
    :param retriable_status_codes:  Set of HTTP status codes that indicate we should retry
    :param args: Args to pass to the API fn
    :return:
    """
    time_waited = 0

    while time_waited < max_wait_time:
        try:
            return api_fn(*args)
        except data_repo_client.ApiException as ae:
            logging.info(ae)
            if ae.status not in retriable_status_codes:
                logging.info(f"data repo api returned status {ae.status}, bailing out")
                raise ae

            logging.info(f"data repo api returned status {ae.status}, retrying")
            time.sleep(poll_interval_seconds)
            time_waited += poll_interval_seconds
    raise DataRepoApiRetryException("Exceeded max wait time for jade job results")
