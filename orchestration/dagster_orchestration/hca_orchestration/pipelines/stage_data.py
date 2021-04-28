from dagster import ModeDefinition, pipeline

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.solids.stage_data import clear_staging_dir, pre_process_metadata, create_staging_dataset
from hca_orchestration.resources import dataflow_beam_runner, local_beam_runner, google_storage_client, \
    jade_data_repo_client, test_beam_runner, local_storage_client, noop_data_repo_client, bigquery_client, \
    noop_bigquery_client, load_tag

from hca_orchestration.resources.config.dataflow_pipelines import staging_bucket_config


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(dataflow_beam_runner, "prod"),
        "storage_client": google_storage_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "bigquery_client": bigquery_client,
        "load_tag": load_tag,
        "staging_bucket_config": staging_bucket_config,
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
        "staging_bucket_config": staging_bucket_config,
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
        "staging_bucket_config": staging_bucket_config,
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
        "staging_bucket_config": staging_bucket_config,
    }
)


@pipeline(
    mode_defs=[prod_mode, dev_mode, local_mode, test_mode]
)
def stage_data() -> None:
    create_staging_dataset(pre_process_metadata(clear_staging_dir()))
