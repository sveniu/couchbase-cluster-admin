import requests


class BaseClient:

    def http_request(self, url, method="GET", data=None, headers={}, timeout=10.0):
        auth = None
        if self.username is not None and self.password is not None:
            auth = (self.username, self.password)

        return requests.request(
            method,
            url,
            data=data,
            headers=headers,
            auth=auth,
            timeout=timeout,
        )
