import json
import subprocess
import time

import jmespath
import pytest
import requests

from couchbase_cluster_admin import cluster

# https://docs.couchbase.com/server/current/install/install-ports.html#detailed-port-description
COUCHBASE_PORT_REST = 8091


@pytest.fixture(scope="session")
def docker_compose_file_path(pytestconfig):
    paths = (
        pytestconfig.rootpath / "tests" / "docker-compose.yml",
        pytestconfig.rootpath / "docker-compose.yml",
    )

    for path in paths:
        if path.is_file():
            return path

    raise FileNotFoundError(f"Could not find docker-compose.yml; tried {paths}")


@pytest.fixture(scope="session")
def docker_inspect(docker_compose_file_path):
    subprocess.check_call(
        [
            "docker-compose",
            "-f",
            docker_compose_file_path,
            "up",
            "-d",
        ]
    )

    yield json.loads(
        subprocess.check_output(
            ["docker", "inspect", "couchbase_node_a", "couchbase_node_b"]
        )
    )

    subprocess.check_call(
        [
            "docker-compose",
            "-f",
            docker_compose_file_path,
            "down",
        ]
    )


def test_status_code(docker_inspect):
    node_a = {
        "host": "127.0.0.1",
        "port": jmespath.search(
            '[0].HostConfig.PortBindings."8091/tcp"[0].HostPort', docker_inspect
        ),
        "internal_ip": jmespath.search(
            "[0].NetworkSettings.Networks.*.IPAddress", docker_inspect
        )[0],
    }
    node_b = {
        "host": "127.0.0.1",
        "port": jmespath.search(
            '[1].HostConfig.PortBindings."8091/tcp"[0].HostPort', docker_inspect
        ),
        "internal_ip": jmespath.search(
            "[1].NetworkSettings.Networks.*.IPAddress", docker_inspect
        )[0],
    }

    # Wait for nodes to start.
    for node in (node_a, node_b):
        while True:
            url = f"""http://{node["host"]}:{node["port"]}/ui/index.html"""
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                pass

            time.sleep(0.5)

    # Enable services on node A.
    c = cluster.Cluster(
        "mycluster",
        services=["kv"],
        api_host=node_a["host"],
        api_port=node_a["port"],
        username="foo",
        password="foobar",  # "The password must be at least 6 characters long."
    )
    c.enable_services()

    # Set memory quotas.
    # {
    #   "errors": {
    #     "memoryQuota": "The data service quota (64MB) cannot be less than 256MB (current total buckets quota, or at least 256MB)."
    #   }
    # }
    c.set_memory_quotas({"memoryQuota": "256"})

    # Set disk paths.
    # "Changing paths of nodes that are part of provisioned cluster is not supported"
    c.set_disk_paths({"path": "/opt/couchbase/test123"})

    # Set authentication.
    c.set_authentication()

    # Join cluster.
    member = cluster.Cluster(
        "mycluster",
        services=["kv"],
        api_host=node_b["host"],
        api_port=node_b["port"],
        username="foo",
        password="foobar",  # "The password must be at least 6 characters long."
    )
    member.enable_services()
    member.join_cluster(node_a["internal_ip"], COUCHBASE_PORT_REST, insecure=True)

    # Assert that we have two nodes, i.e. the join was successful.
    assert len(c.known_nodes) == 2

    # Run the rebalance operation.
    c.rebalance()

    # Check for rebalance completion.
    for _ in range(60):
        if c.rebalance_is_done():
            break
        time.sleep(1)
    else:
        raise AssertionError("Rebalance did not complete in time.")

    # Create a bucket.
    assert len(c.buckets) == 0
    c.create_bucket(
        {
            "name": "mybucket",
            "ramQuotaMB": "100",
        }
    )
    assert len(c.buckets) == 1

    # Create a user.
    assert len(c.users) == 0
    c.create_user(
        "testuser",
        {
            "name": "Test User",
            "password": "testpassword",
            "roles": ["ro_admin"],
        },
    )
    assert len(c.users) == 1
