import logging
import time

import requests


class BaseClient:
    def __init__(self, tls_verify=True):
        self.tls_verify = tls_verify

    def http_request(self, url, method="GET", data=None, json=None, headers={}, timeout=58.0):
        auth = None
        if self.username is not None and self.password is not None:
            auth = (self.username, self.password)

        # Retrying POST requests could lead to unexpected results,
        # but we've seen the join cluster operation (a POST) fail most often.
        max_retries = 3

        while max_retries > 0:
            try:
                max_retries = max_retries - 1
                response = requests.request(
                    method,
                    url,
                    data=data,
                    json=json,
                    headers=headers,
                    auth=auth,
                    timeout=timeout,
                    verify=self.tls_verify,
                )

                return response
            except requests.exceptions.ReadTimeout as e:
                logging.warning(
                    f"ReadTimeout exception for request {method} {url}. "
                    + f"{max_retries} retries left: {e}"
                )
                time.sleep(1)
