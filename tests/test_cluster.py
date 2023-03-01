import pytest
import responses
from responses import matchers

from couchbase_cluster_admin import cluster


@responses.activate
def test_enable_services():
    host = "127.0.0.1"
    port = "8091"

    responses.add(
        responses.POST,
        f"http://{host}:{port}/node/controller/setupServices",
        body="",
        match=[
            matchers.urlencoded_params_matcher(
                {"services": "service1,service2"},
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], api_host=host, api_port=port
    )
    c.enable_services()

    assert len(responses.calls) == 1


@responses.activate
def test_set_memory_quotas():
    host = "127.0.0.1"
    port = "8091"

    responses.add(
        responses.POST,
        f"http://{host}:{port}/pools/default",
        body="",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "clusterName": "mycluster",
                    "service1": "100",
                    "service2": "200",
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], api_host=host, api_port=port
    )
    c.set_memory_quotas({"service1": 100, "service2": 200})

    assert len(responses.calls) == 1


@responses.activate
def test_set_memory_quotas_ratios():
    host = "127.0.0.1"
    port = "8091"

    responses.add(
        responses.POST,
        f"http://{host}:{port}/pools/default",
        body="",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "clusterName": "mycluster",
                    "service1": "10",
                    "service2": "20",
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], api_host=host, api_port=port
    )
    c.set_memory_quotas({"service1": 0.1, "service2": 0.2}, total_memory_mb=100)

    assert len(responses.calls) == 1


def test_set_memory_quotas_ratios_missing_total_raises():
    c = cluster.Cluster(
        "mycluster",
        services=["service1", "service2"],
    )

    with pytest.raises(cluster.IllegalArgumentError):
        c.set_memory_quotas({"service1": 0.1, "service2": 0.2})


@responses.activate
def test_set_cluster_name():
    host = "127.0.0.1"
    port = "8091"

    responses.add(
        responses.POST,
        f"http://{host}:{port}/pools/default",
        body="",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "clusterName": "newclustername",
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster",
        services=["service1", "service2"],
        api_host=host, api_port=port
    )
    c.set_cluster_name("newclustername")

    assert len(responses.calls) == 1


@responses.activate
def test_set_memory_quotas_by_service_name():
    host = "127.0.0.1"
    port = "8091"

    responses.add(
        responses.POST,
        f"http://{host}:{port}/pools/default",
        body="",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "clusterName": "mycluster",
                    "memoryQuota": "100",
                    "indexMemoryQuota": "200",
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], api_host=host, api_port=port
    )
    c.set_memory_quotas_by_service_name({"kv": 100, "index": 200})

    assert len(responses.calls) == 1


@responses.activate
def test_set_authentication():
    host = "127.0.0.1"
    port = "8091"

    responses.add(
        responses.POST,
        f"http://{host}:{port}/settings/web",
        body="",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "username": "foo",
                    "password": "bar",
                    "port": "SAME",
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], api_host=host, api_port=port
    )
    c.set_authentication("foo", "bar")

    assert len(responses.calls) == 1


@responses.activate
def test_set_disk_paths():
    host = "127.0.0.1"
    port = "8091"

    paths = {
        "test1": "/foo/test1",
        "test2": "/foo/test2",
    }
    responses.add(
        responses.POST,
        f"http://{host}:{port}/nodes/self/controller/settings",
        body="",
        match=[matchers.urlencoded_params_matcher(paths)],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], api_host=host, api_port=port
    )
    c.set_disk_paths(paths)

    assert len(responses.calls) == 1


@responses.activate
def test_join_cluster():
    host = "127.0.0.1"
    port = "8091"

    target_ip = "127.0.0.99"
    target_port = "8091"
    username = "foo"
    password = "bar"
    responses.add(
        responses.POST,
        f"http://{host}:{port}/node/controller/doJoinCluster",
        body="",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "clusterMemberHostIp": target_ip,
                    "clusterMemberPort": target_port,
                    "services": "service1,service2",
                    "user": username,
                    "password": password,
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], api_host=host, api_port=port
    )
    c.join_cluster(target_ip, target_port, username, password)

    assert len(responses.calls) == 1


@responses.activate
def test_rebalance():
    host = "127.0.0.1"
    port = "8091"

    known_nodes = ["k1", "k2", "k3"]
    ejected_nodes = ["e1", "e2", "e3"]
    responses.add(
        responses.POST,
        f"http://{host}:{port}/controller/rebalance",
        body="",
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "knownNodes": ",".join(known_nodes),
                    "ejectedNodes": ",".join(ejected_nodes),
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], api_host=host, api_port=port
    )
    c.rebalance(known_nodes, ejected_nodes)

    assert len(responses.calls) == 1
