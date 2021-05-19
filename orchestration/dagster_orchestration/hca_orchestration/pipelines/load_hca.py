from dagster import ModeDefinition, pipeline

from dagster_gcp.gcs import gcs_pickle_io_manager

from dagster_utils.resources.beam import dataflow_beam_runner, local_beam_runner, test_beam_runner
from dagster_utils.resources.bigquery import bigquery_client, noop_bigquery_client
from dagster_utils.resources.google_storage import google_storage_client, mock_storage_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client, noop_data_repo_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources import load_tag
from hca_orchestration.solids.load_hca.load_data_files import import_data_files
from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir, pre_process_metadata, create_scratch_dataset

from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.hca_dataset import target_hca_dataset


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(dataflow_beam_runner, "prod"),
        "bigquery_client": bigquery_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "gcs": google_storage_client,
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(dataflow_beam_runner, "dev"),
        "bigquery_client": bigquery_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset
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
        "target_hca_dataset": target_hca_dataset
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "beam_runner": test_beam_runner,
        "gcs": mock_storage_client,
        "data_repo_client": noop_data_repo_client,
        "bigquery_client": noop_bigquery_client,
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset
    }
)


@pipeline(
    mode_defs=[prod_mode, dev_mode, local_mode, test_mode]
)
def load_hca() -> None:
    staging_dataset = create_scratch_dataset(pre_process_metadata(clear_scratch_dir()))
    import_data_files(staging_dataset)
