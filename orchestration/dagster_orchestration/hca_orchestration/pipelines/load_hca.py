from dagster import ModeDefinition, pipeline, solid, InputDefinition, OutputDefinition

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

HCA_TABLES = [
    'aggregate_generation_protocol',
    'analysis_process',
    'analysis_protocol',
    'cell_line',
    'cell_suspension',
    'collection_protocol',
    'differentiation_protocol',
    'dissociation_protocol',
    'donor_organism',
    'enrichment_protocol',
    'imaged_specimen',
    'imaging_preparation_protocol',
    'imaging_protocol',
    'ipsc_induction_protocol',
    'library_preparation_protocol',
    'organoid',
    'process',
    'project',
    'protocol',
    'sequencing_protocol',
    'specimen_from_organism',
    'links',
]


@solid(
    input_defs=[InputDefinition("upstream_results", list[int])]
)
def collector(_context, upstream_results: list[int]) -> int:
    return 0


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def load_hca_data() -> None:
    preamble = create_staging_dataset(pre_process_metadata(clear_staging_dir()))
    #
    # import_metadata_solids = []
    # for table in HCA_TABLES:
    #     import_metadata_solids.append(import_metadata(table))

    metadata_results = fan_out_to_tables(preamble).map(import_metadata).collect()
    file_data_results = fan_out_to_tables(preamble).map(import_data_files).collect()
    collector([metadata_results, file_data_results])
