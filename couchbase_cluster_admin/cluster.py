import logging
import time

from .client import BaseClient
from .exceptions import (
    AddToNotProvisionedNodeException,
    BackupPlanCreationException,
    BackupRepositoryCreationException,
    BucketCreationException,
    ClusterJoinException,
    ConnectToControllerOnJoinException,
    IllegalArgumentError,
    NodeRenameException,
    RebalanceException,
    SetAuthenticationException,
    SetClusterNameException,
    SetMemoryQuotaException,
    UserCreationException,
)


COUCHBASE_HOST = "127.0.0.1"
COUCHBASE_PORT_REST = "8091"
COUCHBASE_SECURE_PORT_REST = "18091"


service_name_memory_quota_table = {
    "cbas": "cbasMemoryQuota",
    "cbbs": None,  # FIXME not implemented
    "eventing": "eventingMemoryQuota",
    "fts": "ftsMemoryQuota",
    "index": "indexMemoryQuota",
    "kv": "memoryQuota",
    "n1ql": "indexMemoryQuota",
}


class Cluster(BaseClient):
    def __init__(
        self,
        cluster_name: str,
        services: list = ["kv"],
        api_protocol="http",
        api_host=COUCHBASE_HOST,
        api_port=COUCHBASE_PORT_REST,
        username=None,
        password=None,
    ):
        self.cluster_name = cluster_name
        self.services = services
        self.api_protocol = api_protocol
        self.api_host = api_host
        self.api_port = api_port
        self.username = username
        self.password = password

    @property
    def baseurl(self):
        return f"{self.api_protocol}://{self.api_host}:{self.api_port}"

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

    def set_cluster_name(self, cluster_name: str):
        """
        https://docs.couchbase.com/server/current/rest-api/rest-name-cluster.html
        """

        url = f"{self.baseurl}/pools/default"
        payload = {"clusterName": cluster_name}

        resp = self.http_request(url, method="POST", data=payload)
        if resp.status_code != 200:
            raise SetClusterNameException(resp.text)

        self.cluster_name = cluster_name

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
            raise SetMemoryQuotaException(resp.text)

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
            raise SetAuthenticationException(resp.text)

    def set_stats(self, send_stats=False):
        """
        Send usage statistics to Couchbase or not.
        """

        url = f"{self.baseurl}/settings/stats"

        resp = self.http_request(
            url,
            method="POST",
            data={"sendStats": send_stats and "true" or "false"},
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to set stats: {resp.text}")

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

    def rename_node(self, new_hostname: str):
        """
        https://docs.couchbase.com/server/current/rest-api/rest-name-node.html

        It is intended that the node we want to rename is the node we're
        currently connected to via the current Cluster/self instance.
        """
        url = f"{self.baseurl}/node/controller/rename"
        payload = {"hostname": new_hostname}

        resp = self.http_request(url, method="POST", data=payload)
        if resp.status_code != 200:
            raise NodeRenameException(resp.text)

        # From now on, send all requests to the new hostname
        self.api_host = new_hostname

    def join_cluster(
        self,
        target_ip,
        target_port=COUCHBASE_SECURE_PORT_REST,
        username=None,
        password=None,
        insecure=False,
    ):
        """
        https://docs.couchbase.com/server/current/manage/manage-nodes/join-cluster-and-rebalance.html#join-a-cluster-with-the-rest-api
        https://docs.couchbase.com/server/current/rest-api/rest-cluster-joinnode.html
        """

        url = f"{self.baseurl}/node/controller/doJoinCluster"

        # In 7.1+, cluster join is only allowed over secure https connections
        # https://docs.couchbase.com/server/7.1/rest-api/rest-cluster-addnodes.html
        if insecure:
            target_ip = f"http://{target_ip}"
            logging.warning("Insecure join will be rejected by Couchbase >= 7.1")

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
            if "Adding nodes to not provisioned" in resp.text:
                raise AddToNotProvisionedNodeException(resp.text)

            if "Failed to connect to" in resp.text:
                raise ConnectToControllerOnJoinException(resp.text)

        if resp.status_code != 200:
            raise ClusterJoinException(resp.text)

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
            raise RebalanceException(resp.text)

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

    def wait_for_rebalance(self, max_wait=60, interval=1):
        """
        Waits for a rebalance operation to complete
        """
        for _ in range(max_wait):
            if self.rebalance_is_done():
                break
            time.sleep(interval)
        else:
            raise TimeoutError("Rebalance did not complete in time.")

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
            raise BucketCreationException(resp.text)

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
            raise UserCreationException(resp.text)

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

    def set_autofailover(self, settings: dict):
        url = f"{self.baseurl}/settings/autoFailover"
        resp = self.http_request(
            url,
            method="POST",
            data=settings,
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to set auto failover settings: {resp.text}")

    def create_backup_plan(self, plan_name: str, plan_settings: dict):
        """
        https://docs.couchbase.com/server/current/rest-api/backup-rest-api.html

        The plan settings dictionary can be a bit unwieldy. Here's an example
        for a weekly backup plan with full backups on Sundays at 01:15 and
        incremental on every other day at 01:15:

          {
             "description" : "Sample Weekly Backup Plan",
             "name" : "sample_weekly",
             "services" : [
                "data",
                "gsi"
             ],
             "tasks" : [
                {
                   "name" : "daily_task_mo",
                   "options" : null,
                   "schedule" : {
                      "frequency" : 1,
                      "job_type" : "BACKUP",
                      "period" : "MONDAY",
                      "time" : "01:15"
                   },
                   "task_type" : "BACKUP"
                },
                ...
                {
                   "full_backup" : true,
                   "name" : "daily_task_su_f",
                   "options" : null,
                   "schedule" : {
                      "frequency" : 1,
                      "job_type" : "BACKUP",
                      "period" : "SUNDAY",
                      "time" : "01:15"
                   },
                   "task_type" : "BACKUP"
                }
             ]
          }
        """
        # The `_p/backup` prefix causes the request to be routed to the backup
        # service nodes automatically, without using the backup service port.
        url = f"{self.baseurl}/_p/backup/api/v1/plan/{plan_name}"

        if not plan_settings:
            raise ValueError("Backup plan settings must be specified.")

        resp = self.http_request(
            url,
            method="POST",
            data=plan_settings,
        )
        if resp.status_code != 200:
            raise BackupPlanCreationException(resp.text)

    def create_backup_repository(self, repository_name: str, repository_settings: dict):
        """
        https://docs.couchbase.com/server/7.1/rest-api/backup-create-repository.html
        """
        # The `_p/backup` prefix causes the request to be routed to the backup
        # service nodes automatically, without using the backup service port.
        url = f"{self.baseurl}/_p/backup/api/v1/cluster/self/repository/active/{repository_name}"

        if not repository_settings:
            raise ValueError("Backup repository settings must be specified.")

        resp = self.http_request(
            url,
            method="POST",
            data=repository_settings,
        )
        if resp.status_code != 200:
            raise BackupRepositoryCreationException(resp.text)
