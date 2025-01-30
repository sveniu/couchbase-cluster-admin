import json
import subprocess
import time

import jmespath
import pytest
import requests

from couchbase_cluster_admin import cluster

# https://docs.couchbase.com/server/current/install/install-ports.html#detailed-port-description
COUCHBASE_PORT_REST = 8091
COUCHBASE_PORT_REST_TLS = 18091


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
            "docker",
            "compose",
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
            "docker",
            "compose",
            "-f",
            docker_compose_file_path,
            "down",
        ]
    )


# Ignore this warning:
# ../lib/python3.10/site-packages/urllib3/connectionpool.py:1043:
#   InsecureRequestWarning: Unverified HTTPS request is being made to host
#   '127.0.0.1'. Adding certificate verification is strongly advised. See:
#   https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings
@pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")
def test_status_code(docker_inspect):
    node_a = {
        "host": "127.0.0.1",
        "port": jmespath.search(
            '[0].HostConfig.PortBindings."18091/tcp"[0].HostPort', docker_inspect
        ),
        "internal_ip": jmespath.search(
            "[0].NetworkSettings.Networks.*.IPAddress", docker_inspect
        )[0],
    }
    node_b = {
        "host": "127.0.0.1",
        "port": jmespath.search(
            '[1].HostConfig.PortBindings."18091/tcp"[0].HostPort', docker_inspect
        ),
        "internal_ip": jmespath.search(
            "[1].NetworkSettings.Networks.*.IPAddress", docker_inspect
        )[0],
    }

    # Wait for nodes to start.
    for node in (node_a, node_b):
        while True:
            url = f"""https://{node["host"]}:{node["port"]}/ui/index.html"""
            try:
                response = requests.get(url, verify=False)
                if response.status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                pass

            time.sleep(0.5)

    # Enable services on node A.
    c = cluster.Cluster(
        "mycluster",
        services=["kv"],
        api_protocol="https",
        api_tls_verify=False,
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

    # Set usage statistics.
    c.set_stats(send_stats=False)

    # Join cluster.
    member = cluster.Cluster(
        "mycluster",
        services=["kv", "backup"],
        api_protocol="https",
        api_tls_verify=False,
        api_host=node_b["host"],
        api_port=node_b["port"],
        username="foo",
        password="foobar",  # "The password must be at least 6 characters long."
    )
    member.enable_services()
    member.join_cluster(node_a["internal_ip"], COUCHBASE_PORT_REST_TLS)

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

    # Update index settings.
    c.update_index_settings({"storageMode": "plasma"})

    # Set autofailover settings.
    c.set_autofailover({"enabled": "false"})

    # Set new cluster name, then verify.
    new_cluster_name = "newclustername"
    c.set_cluster_name(new_cluster_name)
    assert c.pool_info["clusterName"] == new_cluster_name

    # Set GSI settings: we should get back the correct index storage mode
    gsi_settings = c.set_gsi_settings({"storageMode": "plasma"})
    assert gsi_settings["storageMode"] == "plasma"

    # Create a bucket.
    assert len(c.buckets) == 0
    bucket_name= "mybucket"
    c.create_bucket(
        {
            "name": bucket_name,
            "ramQuotaMB": "100",
        }
    )
    assert len(c.buckets) == 1

    # Create a scope.
    scope_name = "myscope"
    c.create_scope(bucket_name, {"name": scope_name})
    got_scopes = c.get_scopes(bucket_name)
    assert scope_name in [s["name"] for s in got_scopes["scopes"]]

    # Create two collections.
    c.create_collection(bucket_name, scope_name, {"name": "mycollection1"})
    c.create_collection(bucket_name, scope_name, {"name": "mycollection2"})
    got_scopes = c.get_scopes(bucket_name)
    assert len(got_scopes["scopes"][0]["collections"]) == 2
    assert "mycollection1" in [c["name"] for c in got_scopes["scopes"][0]["collections"]]
    assert "mycollection2" in [c["name"] for c in got_scopes["scopes"][0]["collections"]]

    # Set bucket property.
    # FIXME only works on localhost. Returned error:
    #   API is accessible from localhost only
    # c.set_bucket_prop("mybucket", "access_scanner_enabled", "false")

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

    # Get cluster backup info.
    backup_info = member.get_backup_info()
    assert {"name", "active", "imported", "archived"} == backup_info.keys()
