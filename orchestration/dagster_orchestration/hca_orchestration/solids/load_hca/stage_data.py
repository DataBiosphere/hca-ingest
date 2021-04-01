import re

from dagster import composite_solid, solid, InputDefinition, Nothing, String, OutputDefinition
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

STAGING_BUCKET_CONFIG_SCHEMA = {
    "staging_bucket_name": String,
    "staging_prefix_name": String,
}


@solid(
    required_resource_keys={"storage_client"},
    config_schema=STAGING_BUCKET_CONFIG_SCHEMA,
)
def clear_staging_dir(context: AbstractComputeExecutionContext) -> int:
    """
    Given a staging bucket + prefix, deletes all blobs present at that path
    :return: Number of deletions
    """

    staging_bucket_name = context.solid_config["staging_bucket_name"]
    staging_prefix_name = context.solid_config["staging_prefix_name"]

    blobs = context.resources.storage_client.list_blobs(staging_bucket_name, prefix=f"{staging_prefix_name}/")
    deletions_count = 0
    for blob in blobs:
        blob.delete()
        deletions_count += 1
    context.log.debug(f"--clear_staging_dir deleted {deletions_count} blobs under {staging_prefix_name}")
    return deletions_count


@solid(
    required_resource_keys={"beam_runner"},
    config_schema={
        **STAGING_BUCKET_CONFIG_SCHEMA,
        "input_prefix": String,
    },
    input_defs=[InputDefinition("start", Nothing)],
)
def pre_process_metadata(context: AbstractComputeExecutionContext) -> Nothing:
    """
    Runs the Beam hca transformation pipeline flow over the given input prefix
    """
    context.log.info("--pre_process_metadata")

    # not strictly required, but makes the ensuing lines a lot shorter
    bucket_name = context.solid_config['staging_bucket_name']
    prefix_name = context.solid_config['staging_prefix_name']

    kebabified_output_prefix = re.sub(r"[^A-Za-z0-9]", "-", prefix_name)

    context.resources.beam_runner.run(
        job_name=f"hca-stage-metadata-{kebabified_output_prefix}",
        input_prefix=context.solid_config["input_prefix"],
        output_prefix=f'gs://{bucket_name}/{prefix_name}'
    )


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    output_defs=[OutputDefinition(name="staging_dataset_name", dagster_type=str)]
)
def create_staging_dataset(context: AbstractComputeExecutionContext) -> str:
    return "fake_dataset_name"


@solid(
    input_defs=[InputDefinition("nothing", Nothing)],
    output_defs=[DynamicOutputDefinition(str)]
)
def fan_out_to_tables(_) -> str:
    tables = [
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
    for table in tables:
        yield DynamicOutput(value=table, mapping_key=table)


@solid(
    input_defs=[InputDefinition("table_name", str)]
)
def diff_against_existing_data(context, table_name) -> Nothing:
    context.log.info(
        f"diff_against_existing_data; table_name = {table_name}"
    )


@solid(
    input_defs=[InputDefinition("nothing", Nothing)]
)
def query_rows_to_append(context) -> Nothing:
    pass


@solid(
    input_defs=[InputDefinition("nothing", Nothing)]
)
def export_appends(context) -> Nothing:
    pass


@solid(
    input_defs=[InputDefinition("nothing", Nothing)]
)
def ingest_metadata_to_jade(context) -> Nothing:
    pass


@composite_solid(
    input_defs=[InputDefinition("table_name", str)]
    #output_defs=[OutputDefinition(name="ignore", dagster_type=Nothing)]
)
def import_metadata(table_name: str) -> Nothing:
    return ingest_metadata_to_jade(
        export_appends(query_rows_to_append(diff_against_existing_data(table_name))))
