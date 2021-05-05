from dagster import ModeDefinition, pipeline

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir, pre_process_metadata, create_scratch_dataset
from hca_orchestration.solids.load_hca.load_data_files import import_data_files
from hca_orchestration.resources import dataflow_beam_runner, local_beam_runner, google_storage_client, \
    jade_data_repo_client, test_beam_runner, local_storage_client, noop_data_repo_client, bigquery_client, \
    noop_bigquery_client, load_tag

from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.hca_dataset import target_hca_dataset


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(dataflow_beam_runner, "prod"),
        "storage_client": google_storage_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "bigquery_client": bigquery_client,
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(dataflow_beam_runner, "dev"),
        "storage_client": google_storage_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "bigquery_client": bigquery_client,
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "beam_runner": local_beam_runner,
        "storage_client": google_storage_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "bigquery_client": bigquery_client,
        "load_tag": load_tag,
        "scratch_config": scratch_config,
        "target_hca_dataset": target_hca_dataset
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "beam_runner": test_beam_runner,
        "storage_client": local_storage_client,
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
