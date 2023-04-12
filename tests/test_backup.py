import responses
from responses import matchers

from couchbase_cluster_admin import cluster


@responses.activate
def test_create_backup_plan():
    host = "127.0.0.1"
    port = "8091"

    plan_name = "test_plan"
    plan_dict = {
        "name": plan_name,
        "description": "Test Plan",
        "tasks": [],
    }

    responses.add(
        responses.POST,
        f"http://{host}:{port}/_p/backup/api/v1/plan/{plan_name}",
        match=[matchers.json_params_matcher(plan_dict)],
        status=200,
    )

    c = cluster.Cluster("mycluster", services=["kv"], api_host=host, api_port=port)
    c.create_backup_plan(plan_name, plan_dict)

    assert len(responses.calls) == 1


@responses.activate
def test_create_backup_repository():
    host = "127.0.0.1"
    port = "8091"

    repo_name = "test_plan"
    repo_settings = {
        "plan": "_hourly_backups",
        "archive": "/Users/user/Documents/archives/testRepo",
        "bucket_name": "travel-sample",
    }

    responses.add(
        responses.POST,
        f"http://{host}:{port}/_p/backup/api/v1/cluster/self/repository/active/{repo_name}",
        match=[matchers.json_params_matcher(repo_settings)],
        status=200,
    )

    c = cluster.Cluster("mycluster", services=["kv"], api_host=host, api_port=port)
    c.create_backup_repository(repo_name, repo_settings)

    assert len(responses.calls) == 1
