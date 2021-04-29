from dagster import ModeDefinition, pipeline

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources import console_slack_client, live_slack_client
from hca_orchestration.resources.data_repo import jade_data_repo_client, noop_data_repo_client
from hca_orchestration.resources.config.data_repo import hca_manage_config, hca_dataset_operation_config
from hca_orchestration.solids.validate_egress import post_import_validate, notify_slack_of_egress_validation_results


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "prod"),
        "hca_dataset_operation_config": hca_dataset_operation_config,
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "hca_dataset_operation_config": hca_dataset_operation_config,
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "slack": console_slack_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "hca_dataset_operation_config": hca_dataset_operation_config,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client,
        "slack": console_slack_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "test"),
        "hca_dataset_operation_config": hca_dataset_operation_config,
    }
)


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def validate_egress() -> None:
    notify_slack_of_egress_validation_results(post_import_validate())
