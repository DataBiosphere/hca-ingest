from dagster import ModeDefinition, pipeline, failure_hook, HookContext

from dagster_utils.resources.google_storage import google_storage_client, mock_storage_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client, noop_data_repo_client
from dagster_utils.resources.slack import console_slack_client, live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.contrib.slack import key_value_slack_blocks
from hca_orchestration.solids.validate_ingress import pre_flight_validate, notify_slack_of_ingress_validation_results

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "gcs": google_storage_client,
        "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "slack": console_slack_client,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client,
        "gcs": mock_storage_client,
        "slack": console_slack_client,
    }
)


@failure_hook(
    required_resource_keys={'slack', 'dagit_config'}
)
def validation_failed_notification(context: HookContext) -> None:
    kvs = {
        "Dagit link": f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'
    }
    context.resources.slack.send_message(blocks=key_value_slack_blocks("Validation Ingress Failed", key_values=kvs))


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode, dev_mode]
)
def validate_ingress() -> None:
    notify_slack_of_ingress_validation_results(pre_flight_validate())
