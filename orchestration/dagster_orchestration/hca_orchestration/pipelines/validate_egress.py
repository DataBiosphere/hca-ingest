from dagster import ModeDefinition, pipeline

from hca_orchestration.resources import jade_data_repo_client, noop_data_repo_client, console_slack_client,\
    live_slack_client
from hca_orchestration.solids.validate_egress import post_import_validate, notify_slack_of_egress_validation_results


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": jade_data_repo_client,
        "slack": live_slack_client,
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": jade_data_repo_client,
        "slack": console_slack_client,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client,
        "slack": console_slack_client,
    }
)


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def validate_egress():
    notify_slack_of_egress_validation_results(post_import_validate())
