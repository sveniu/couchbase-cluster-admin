import pytest
import requests
import responses
from couchbase_cluster_admin import cluster
from responses import matchers


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
        "mycluster", services=["service1", "service2"], host=host, port=port
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
                    "service1": "100",
                    "service2": "200",
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], host=host, port=port
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
                    "service1": "10",
                    "service2": "20",
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], host=host, port=port
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
                    "memoryQuota": "100",
                    "indexMemoryQuota": "200",
                }
            )
        ],
        status=200,
    )

    c = cluster.Cluster(
        "mycluster", services=["service1", "service2"], host=host, port=port
    )
    c.set_memory_quotas_by_service_name({"kv": 100, "index": 200})

    assert len(responses.calls) == 1
