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
