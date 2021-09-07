from dagster import ModeDefinition, pipeline
from dagster import ResourceDefinition
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner
from dagster_utils.resources.beam.local_beam_runner import local_beam_runner
from dagster_utils.resources.beam.noop_beam_runner import noop_beam_runner
from dagster_utils.resources.bigquery import bigquery_client, noop_bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client, noop_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client, mock_storage_client
from dagster_utils.resources.slack import live_slack_client, console_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources import load_tag, bigquery_service, mock_bigquery_service
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import target_hca_dataset
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.solids.load_hca.data_files.load_data_files import import_data_files
from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import file_metadata_fanout
from hca_orchestration.solids.load_hca.non_file_metadata.load_non_file_metadata import non_file_metadata_fanout
from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir, pre_process_metadata, create_scratch_dataset
from hca_orchestration.solids.load_hca.utilities import initial_solid, terminal_solid

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod"),
        "bigquery_client": bigquery_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "gcs": google_storage_client,
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset,
        "bigquery_service": bigquery_service,
        "data_repo_service": data_repo_service,
        "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "prod")
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "dev"),
        "bigquery_client": bigquery_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset,
        "bigquery_service": bigquery_service,
        "data_repo_service": data_repo_service,
        "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "dev")
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "beam_runner": local_beam_runner,
        "bigquery_client": bigquery_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset,
        "bigquery_service": bigquery_service,
        "data_repo_service": data_repo_service,
        "slack": console_slack_client,
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "local")
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "beam_runner": noop_beam_runner,
        "gcs": mock_storage_client,
        "data_repo_client": noop_data_repo_client,
        "bigquery_client": noop_bigquery_client,
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset,
        "bigquery_service": mock_bigquery_service,
        "data_repo_service": ResourceDefinition.mock_resource(),
        "slack": console_slack_client,
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "test")
    }
)


@pipeline(
    mode_defs=[prod_mode, dev_mode, local_mode, test_mode]
)
def load_hca() -> None:
    staging_dataset = create_scratch_dataset(
        pre_process_metadata(
            clear_scratch_dir(
                initial_solid()
            )))

    result = import_data_files(staging_dataset).collect()

    file_metadata_results = file_metadata_fanout(result, staging_dataset).collect()
    non_file_metadata_results = non_file_metadata_fanout(result, staging_dataset).collect()

    terminal_solid(file_metadata_results, non_file_metadata_results)