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
