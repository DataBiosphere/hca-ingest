from dagster import ModeDefinition, pipeline

from hca_orchestration.solids.stage_data import clear_staging_dir, pre_process_metadata
from hca_orchestration.resources import dataflow_beam_runner, local_beam_runner, google_storage_client, \
    jade_data_repo_client, test_beam_runner, local_storage_client, noop_data_repo_client


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": dataflow_beam_runner,
        "storage_client": google_storage_client,
        "data_repo_client": jade_data_repo_client
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "beam_runner": local_beam_runner,
        "storage_client": google_storage_client,
        "data_repo_client": jade_data_repo_client
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "beam_runner": test_beam_runner,
        "storage_client": local_storage_client,
        "data_repo_client": noop_data_repo_client
    }
)


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def stage_data() -> None:
    pre_process_metadata(start=clear_staging_dir())
