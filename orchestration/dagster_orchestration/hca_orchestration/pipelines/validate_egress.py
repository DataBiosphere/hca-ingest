from dagster import ModeDefinition, pipeline

from dagster_gcp.gcs import gcs_pickle_io_manager

from dagster_utils.resources.google_storage import google_storage_client, mock_storage_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client, noop_data_repo_client
from dagster_utils.resources.slack import console_slack_client, live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources.config.data_repo import hca_manage_config, hca_dataset_operation_config
from hca_orchestration.solids.validate_egress import post_import_validate, notify_slack_of_egress_validation_results


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "prod"),
        "hca_dataset_operation_config": hca_dataset_operation_config,
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
        "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "hca_dataset_operation_config": hca_dataset_operation_config,
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "hca_dataset_operation_config": hca_dataset_operation_config,
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        "slack": console_slack_client,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client,
        "gcs": mock_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "test"),
        "hca_dataset_operation_config": hca_dataset_operation_config,
        "slack": console_slack_client,
    }
)


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def validate_egress() -> None:
    notify_slack_of_egress_validation_results(post_import_validate())
