from time import sleep
import requests
import logging


class MaxRetriesExceededError(Exception):
    """Raised when the maximum number of retries is exceeded."""


class HTTPClient:
    def __init__(
        self, base_url, http_codes_non_retriable=[400, 404, 422], max_retries=4, timeout=30, backoff=30, backoff_max=30
    ):
        self.base_url = base_url
        self.max_retries = max_retries
        self.timeout = timeout
        self.backoff = backoff
        self.backoff_max = backoff_max
        self.http_codes_non_retriable = http_codes_non_retriable

        # Configure the logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def retry_request(func):
        def wrapper(self, *args, **kwargs):
            url = f"{self.base_url}{args[0]}"
            retries_count = 0

            def _is_retriable(response):
                # Response is None due to request timeout
                # Response status_code is not in non-retriable list and response is not successful
                return (response is None) or (response.status_code not in self.http_codes_non_retriable and not response.ok)

            def _send_request():
                try:
                    response = func(self, *args, **kwargs)
                    if response.ok:
                        logging.debug(f"Request processed for URL: {url}; HTTP response status code: {response.status_code}")
                    else:
                        logging.info(f"Request processed for URL: {url}; HTTP response status code: {response.status_code}")
                except requests.exceptions.Timeout:
                    response = None
                    logging.info(f"Request for URL: {url} timed out.")
                return response

            def _wait_before_retry():
                if retries_count < 1:
                    return
                delay = min(self.backoff * 2**retries_count, self.backoff_max)
                logging.info(f"Waiting {delay} seconds before next attempt.")
                sleep(delay)

            response = _send_request()
            while retries_count < self.max_retries and _is_retriable(response):
                _wait_before_retry()
                retries_count += 1
                logging.info(f"Retry attempt: {retries_count}")
                response = _send_request()

            if response is not None:
                return response

            # Raise exception if request timed out after max number of retries.
            raise MaxRetriesExceededError(f"Request for URL: {url} failed after {retries_count} retries.")

        return wrapper

    @retry_request
    def get(self, endpoint, params=None, headers=None):
        return requests.get(
            url=f"{self.base_url}{endpoint}",
            params=params,
            timeout=self.timeout,
            headers=headers,
        )

    @retry_request
    def post(self, endpoint=None, data=None, json=None, headers=None):
        return requests.post(
            url=f"{self.base_url}{endpoint}",
            data=data,
            json=json,
            timeout=self.timeout,
            headers=headers,
        )
