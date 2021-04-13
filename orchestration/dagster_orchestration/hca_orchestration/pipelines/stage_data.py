from dagster import ModeDefinition, pipeline

from hca_orchestration.solids.stage_data import clear_staging_dir, pre_process_metadata, create_staging_dataset
from hca_orchestration.resources import dataflow_beam_runner, local_beam_runner, google_storage_client, \
    jade_data_repo_client, test_beam_runner, local_storage_client, noop_data_repo_client, bigquery_client, \
    noop_bigquery_client, load_tag


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": dataflow_beam_runner,
        "storage_client": google_storage_client,
        "data_repo_client": jade_data_repo_client,
        "bigquery_client": bigquery_client,
        "load_tag": load_tag,
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "beam_runner": local_beam_runner,
        "storage_client": google_storage_client,
        "data_repo_client": jade_data_repo_client,
        "bigquery_client": bigquery_client,
        "load_tag": load_tag,
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
    }
)


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def stage_data() -> None:
    create_staging_dataset(pre_process_metadata(clear_staging_dir()))
