import requests

COUCHBASE_HOST = "127.0.0.1"
COUCHBASE_PORT_REST = "8091"


class BaseClient:
    def __init__():
        pass

    def http_request(self, url, method="GET", data=None, headers=None, timeout=10.0):
        if headers is None:
            headers = {}

        return requests.request(method, url, data=data, headers=headers)


class Cluster(BaseClient):
    def __init__(
        self,
        cluster_id: str,
        services: list = ["kv"],
        host=COUCHBASE_HOST,
        port=COUCHBASE_PORT_REST,
    ):
        self.cluster_id = cluster_id
        self.services = services
        self.baseurl = f"http://{host}:{port}"

    def enable_services(self):
        """
        https://docs.couchbase.com/server/current/manage/manage-nodes/create-cluster.html#provision-a-node-with-the-rest-api
        https://docs.couchbase.com/server/current/rest-api/rest-node-services.html
        """

        url = f"{self.baseurl}/node/controller/setupServices"
        resp = self.http_request(
            url,
            method="POST",
            data={
                "services": ",".join(self.services),
            },
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to enable services: {resp.text}")
