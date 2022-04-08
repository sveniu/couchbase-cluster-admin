import pytest
import requests
from couchbase_cluster_admin import cluster
from requests.exceptions import ConnectionError

# https://docs.couchbase.com/server/current/install/install-ports.html#detailed-port-description
COUCHBASE_PORT_REST = 8091


def is_ready(url, want_status_code=200):
    try:
        response = requests.get(url)
        if response.status_code == want_status_code:
            return True
    except ConnectionError:
        return False


@pytest.fixture(scope="session")
def couchbase_rest(docker_ip, docker_services):
    """Ensure that HTTP service is up and responsive."""

    # Map the given container port to the corresponding host port.
    port = docker_services.port_for("couchbase1", COUCHBASE_PORT_REST)

    # Wait until the service is ready.
    docker_services.wait_until_responsive(
        # Note: Fetching / will return a 301 redirect to /ui/index.html, which
        # will be followed by requests (allow_redirects enabled by default).
        timeout=30.0,
        pause=0.1,
        check=lambda: is_ready(f"http://{docker_ip}:{port}"),
    )

    return {"host": docker_ip, "port": port}


def test_status_code(couchbase_rest):
    url = f"""http://{couchbase_rest["host"]}:{couchbase_rest["port"]}/ui/index.html"""
    response = requests.get(url)

    assert response.status_code == 200


def test_couchbase_enable_services(couchbase_rest):
    c = cluster.Cluster(
        "mycluster",
        services=["kv"],
        host=couchbase_rest["host"],
        port=couchbase_rest["port"],
    )
    c.enable_services()


def test_couchbase_set_memory_quotas(couchbase_rest):
    c = cluster.Cluster(
        "mycluster",
        services=["kv"],
        host=couchbase_rest["host"],
        port=couchbase_rest["port"],
    )
    # {
    #   "errors": {
    #     "memoryQuota": "The data service quota (64MB) cannot be less than 256MB (current total buckets quota, or at least 256MB)."
    #   }
    # }
    c.set_memory_quotas({"memoryQuota": "256"})


def test_couchbase_set_disk_paths(couchbase_rest):
    c = cluster.Cluster(
        "mycluster",
        services=["kv"],
        host=couchbase_rest["host"],
        port=couchbase_rest["port"],
        username="foo",
        password="foobar",
    )
    # "Changing paths of nodes that are part of provisioned cluster is not supported"
    c.set_disk_paths({"path": "/opt/couchbase/test123"})


def test_couchbase_set_authentication(couchbase_rest):
    c = cluster.Cluster(
        "mycluster",
        services=["kv"],
        host=couchbase_rest["host"],
        port=couchbase_rest["port"],
        username="foo",
        password="foobar",  # "The password must be at least 6 characters long."
    )
    c.set_authentication()
