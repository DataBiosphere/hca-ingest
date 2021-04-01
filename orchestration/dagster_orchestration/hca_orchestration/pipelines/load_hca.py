from dagster import ModeDefinition, pipeline

from hca_orchestration.solids.load_hca.stage_data import \
    clear_staging_dir, \
    pre_process_metadata, \
    create_staging_dataset
from hca_orchestration.solids.load_hca.load_metadata import \
    fan_out_to_tables, import_metadata
from hca_orchestration.solids.load_hca.load_data_files import \
    import_data_files
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
def load_hca_data() -> None:
    preamble = create_staging_dataset(pre_process_metadata(clear_staging_dir()))
    metadata_results = fan_out_to_tables(preamble).map(import_metadata)

    fan_out_to_tables(metadata_results.collect()).map(import_data_files)
    # import_data_files(metadata_results.collect())
