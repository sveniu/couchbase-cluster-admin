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


class AddToNotProvisionedNodeException(Exception):
    pass


class ConnectToControllerOnJoinException(Exception):
    pass


class BaseClient:
    def __init__():
        pass

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


class Cluster(BaseClient):
    def __init__(
        self,
        cluster_name: str,
        services: list = ["kv"],
        api_host=COUCHBASE_HOST,
        api_port=COUCHBASE_PORT_REST,
        username=None,
        password=None,
    ):
        self.cluster_name = cluster_name
        self.services = services
        self.baseurl = f"http://{api_host}:{api_port}"
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

        # The cluster name is also set using this endpoint, but it's not
        # documented. Found by tracing the web UI.
        quotas["clusterName"] = self.cluster_name

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
                "services": ",".join(self.services),
                "user": self.username if username is None else username,
                "password": self.password if password is None else password,
            },
        )

        if resp.status_code == 400:
            if "Adding nodes to not provisioned" in resp.content:
                raise AddToNotProvisionedNodeException(resp.text)

            if "Failed to connect to" in resp.content:
                raise ConnectToControllerOnJoinException(resp.text)

        if resp.status_code != 200:
            raise Exception(f"Failed to join cluster: {resp.text}")

    @property
    def node_info(self):
        """
        https://docs.couchbase.com/server/current/rest-api/rest-getting-storage-information.html
        """

        url = f"{self.baseurl}/nodes/self"
        resp = self.http_request(url)
        if resp.status_code != 200:
            raise Exception(f"Failed to get node info: {resp.text}")

        return resp.json()

    @property
    def node_name(self):
        return self.node_info["otpNode"]

    @property
    def node_uuid(self):
        return self.node_info["nodeUUID"]

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

    @property
    def known_nodes(self):
        return [node["otpNode"] for node in self.pool_info["nodes"]]

    def rebalance(
        self,
        known_nodes=None,
        ejected_nodes=[],
    ):
        """
        https://docs.couchbase.com/server/current/manage/manage-nodes/join-cluster-and-rebalance.html#join-a-cluster-with-the-rest-api
        https://docs.couchbase.com/server/current/rest-api/rest-cluster-rebalance.html
        """

        # If no known nodes are specified, use the current list of known nodes.
        if known_nodes is None:
            known_nodes = self.known_nodes

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

    @property
    def buckets(self):
        url = f"{self.baseurl}/pools/default/buckets"
        resp = self.http_request(url)
        if resp.status_code != 200:
            raise Exception(f"Failed to get buckets: {resp.text}")

        return resp.json()

    def create_bucket(self, bucket_config: dict):
        url = f"{self.baseurl}/pools/default/buckets"
        resp = self.http_request(
            url,
            method="POST",
            data=bucket_config,
        )
        if resp.status_code not in (200, 202):
            raise Exception(f"Failed to create bucket: {resp.text}")

    @property
    def users(self):
        url = f"{self.baseurl}/settings/rbac/users"
        resp = self.http_request(url)
        if resp.status_code != 200:
            raise Exception(f"Failed to get users: {resp.text}")

        return resp.json()

    def create_user(self, username, user_config: dict):
        url = f"{self.baseurl}/settings/rbac/users/local/{username}"

        data = user_config

        # Transform list to comma-separated values.
        if "roles" in data:
            data["roles"] = ",".join(data["roles"])

        resp = self.http_request(
            url,
            method="PUT",
            data=data,
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to create user: {resp.text}")

    def diag_eval(self, data: bytes):
        url = f"{self.baseurl}/diag/eval"

        resp = self.http_request(
            url,
            method="POST",
            data=data,
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to eval: {resp.text}")

    def set_bucket_prop(self, bucket, prop, val):
        data = (
            f'ns_bucket:update_bucket_props("{bucket}", '
            "[{extra_config_string, "
            f'"{prop}={val}"'
            "}])"
        ).encode("utf-8")
        return self.diag_eval(data)

    def update_index_settings(self, settings: dict):
        url = f"{self.baseurl}/settings/indexes"
        resp = self.http_request(
            url,
            method="POST",
            data=settings,
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to update index settings: {resp.text}")
