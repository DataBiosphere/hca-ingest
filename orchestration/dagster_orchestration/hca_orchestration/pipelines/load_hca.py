from dagster import ModeDefinition, pipeline, solid, InputDefinition, Nothing, PresetDefinition, file_relative_path
from hca_orchestration.pipelines import utils
from hca_orchestration.resources import dataflow_beam_runner, local_beam_runner, google_storage_client, \
    jade_data_repo_client, test_beam_runner, local_storage_client, noop_data_repo_client
from hca_orchestration.solids.load_hca.load_data_files import \
    import_data_files
from hca_orchestration.solids.load_hca.load_metadata import \
    import_metadata
from hca_orchestration.solids.load_hca.stage_data import \
    clear_staging_dir, \
    pre_process_metadata, \
    create_staging_dataset
from hca_orchestration.resources.slack import console_slack_client
from hca_orchestration.resources.bigquery import bigquery_client

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": dataflow_beam_runner,
        "storage_client": google_storage_client,
        "bigquery_client": bigquery_client,
        "data_repo_client": jade_data_repo_client,
        "slack": console_slack_client,
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "beam_runner": local_beam_runner,
        "storage_client": google_storage_client,
        "bigquery_client": bigquery_client,
        "data_repo_client": jade_data_repo_client,
        "slack": console_slack_client
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "beam_runner": test_beam_runner,
        "storage_client": local_storage_client,
        "bigquery_client": bigquery_client,
        "data_repo_client": noop_data_repo_client,
        "slack": console_slack_client,
    }
)

HCA_FILE_TABLES = [
    'analysis_file',
    'image_file',
    'reference_file',
    'sequence_file',
    'supplementary_file'
]

HCA_METADATA_TABLES = [
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
    input_defs=[
        InputDefinition("metadata_fanin", Nothing),
        InputDefinition("datafile_fanin", Nothing)
    ],
    required_resource_keys={"slack"}
)
def post_process(context) -> Nothing:
    context.resources.slack.chat_postMessage(
        channel="monster-ci",
        text="HCA import complete")


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode],
    preset_defs=[PresetDefinition.from_files(
        "local_dev_testing",
        config_files=[
            file_relative_path(__file__, "../environments/preset_local_dev_testing.yaml")
        ],
        mode="local",
    )]
)
def load_hca_data() -> None:
    preamble = create_staging_dataset(pre_process_metadata(clear_staging_dir()))

    metadata_file_fanout = utils.build_table_fanout(HCA_METADATA_TABLES, name="fanout_metadata_file_tables")

    # TODO we have to build intermediate fan-in solids because we cannot use
    # the results of 2 collect operations directly as input to a downstream solid
    # https://dagster.slack.com/archives/CCCR6P2UR/p1617383598383600
    data_file_fanin = utils.build_table_fanin("fanin_data_file_tables")
    metadata_file_fanin = utils.build_table_fanin("fanin_metadata_file_tables")

    data_file_results = import_data_files(preamble)
    metadata_results = metadata_file_fanout(preamble).map(import_metadata).collect()

    post_process(
        metadata_fanin=metadata_file_fanin(metadata_results),
        datafile_fanin=data_file_fanin(data_file_results)
    )
