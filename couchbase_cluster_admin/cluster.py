import requests

COUCHBASE_HOST = "127.0.0.1"
COUCHBASE_PORT_REST = "8091"


service_name_memory_quota_table = {
    "cbas": "cbasMemoryQuota",
    "cbbs": None,  # FIXME not implemented
    "eventing": "eventingMemoryQuota",
    "fts": "ftsMemoryQuota",
    "index": "indexMemoryQuota",
    "kv": "memoryQuota",
    "n1ql": "indexMemoryQuota",
}


class IllegalArgumentError(ValueError):
    pass


class BaseClient:
    def __init__():
        pass

    def http_request(self, url, method="GET", data=None, headers=None, timeout=10.0):
        if headers is None:
            headers = {}

        auth = None
        if self.username is not None and self.password is not None:
            auth = (self.username, self.password)

        return requests.request(method, url, data=data, headers=headers, auth=auth)


class Cluster(BaseClient):
    def __init__(
        self,
        cluster_id: str,
        services: list = ["kv"],
        host=COUCHBASE_HOST,
        port=COUCHBASE_PORT_REST,
        username=None,
        password=None,
    ):
        self.cluster_id = cluster_id
        self.services = services
        self.baseurl = f"http://{host}:{port}"
        self.username = username
        self.password = password

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

    def set_memory_quotas(self, quotas: dict, total_memory_mb: int = None):
        """
        https://docs.couchbase.com/server/current/manage/manage-nodes/create-cluster.html#provision-a-node-with-the-rest-api
        https://docs.couchbase.com/server/current/rest-api/rest-node-memory-quota.html

        Values are in megabytes, or ratios of total memory.
        """

        url = f"{self.baseurl}/pools/default"

        # Transform quota ratios to absolute values.
        for quota_name, value in quotas.items():
            if isinstance(value, float):
                if total_memory_mb is None:
                    raise IllegalArgumentError("total_memory_mb is required for ratios")
                quotas[quota_name] = int(value * total_memory_mb)

        resp = self.http_request(
            url,
            method="POST",
            data=quotas,
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to set memory quotas: {resp.text}")

    def set_memory_quotas_by_service_name(self, quotas_by_service_name: dict, *args):
        """
        Convenience function for setting memory quotas by service name.

        Values are in megabytes, or ratios of total memory.
        """

        quotas = {}
        for service_name, value in quotas_by_service_name.items():
            if service_name not in service_name_memory_quota_table:
                raise ValueError(f"Unknown service name: {service_name}")
            quotas[service_name_memory_quota_table[service_name]] = value

        self.set_memory_quotas(quotas, *args)

    def set_authentication(self, username=None, password=None):
        """
        https://docs.couchbase.com/server/current/manage/manage-nodes/create-cluster.html#provision-a-node-with-the-rest-api
        https://docs.couchbase.com/server/current/rest-api/rest-node-set-username.html
        """

        url = f"{self.baseurl}/settings/web"

        if username is not None:
            self.username = username

        if password is not None:
            self.password = password

        resp = self.http_request(
            url,
            method="POST",
            data={
                "username": self.username,
                "password": self.password,
                "port": "SAME",
            },
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to set authentication: {resp.text}")

    def set_disk_paths(self, disk_paths: dict):
        """
        https://docs.couchbase.com/server/current/manage/manage-nodes/initialize-node.html#initialize-a-node-with-the-rest-api
        https://docs.couchbase.com/server/current/rest-api/rest-node-index-path.html

        TODO "This parameter [cbas_path] can be repeated several times,
        separated by ampersands, to setup multiple storage paths (I/O devices)
        in analytics."
        """

        url = f"{self.baseurl}/nodes/self/controller/settings"
        resp = self.http_request(
            url,
            method="POST",
            data=disk_paths,
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to set disk paths: {resp.text}")

    def join_cluster(
        self,
        target_ip,
        target_port=COUCHBASE_PORT_REST,
        username=None,
        password=None,
        insecure=False,
    ):
        """
        https://docs.couchbase.com/server/current/manage/manage-nodes/join-cluster-and-rebalance.html#join-a-cluster-with-the-rest-api
        https://docs.couchbase.com/server/current/rest-api/rest-cluster-joinnode.html
        """

        url = f"{self.baseurl}/node/controller/doJoinCluster"

        if insecure:
            target_ip = f"http://{target_ip}"

        resp = self.http_request(
            url,
            method="POST",
            data={
                "clusterMemberHostIp": target_ip,
                "clusterMemberPort": target_port,
                "user": self.username if username is None else username,
                "password": self.password if password is None else password,
            },
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to join cluster: {resp.text}")

    @property
    def pool_info(self):
        """
        https://docs.couchbase.com/server/current/rest-api/rest-cluster-details.html
        """

        url = f"{self.baseurl}/pools/default"
        resp = self.http_request(url)
        if resp.status_code != 200:
            raise Exception(f"Failed to get pool info: {resp.text}")

        return resp.json()

    def rebalance(
        self,
        known_nodes=[],
        ejected_nodes=[],
    ):
        """
        https://docs.couchbase.com/server/current/manage/manage-nodes/join-cluster-and-rebalance.html#join-a-cluster-with-the-rest-api
        https://docs.couchbase.com/server/current/rest-api/rest-cluster-rebalance.html
        """

        data = {}
        if known_nodes:
            data["knownNodes"] = ",".join(known_nodes)
        if ejected_nodes:
            data["ejectedNodes"] = ",".join(ejected_nodes)

        url = f"{self.baseurl}/controller/rebalance"

        resp = self.http_request(
            url,
            method="POST",
            data=data,
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to join cluster: {resp.text}")

    @property
    def rebalance_progress(self):
        """
        https://docs.couchbase.com/server/current/rest-api/rest-get-rebalance-progress.html
        """

        url = f"{self.baseurl}/pools/default/rebalanceProgress"

        resp = self.http_request(url)
        if resp.status_code != 200:
            raise Exception(f"Failed to get rebalance progress: {resp.text}")

        return resp.json()

    def rebalance_is_done(self) -> bool:
        return self.rebalance_progress["status"] == "none"
