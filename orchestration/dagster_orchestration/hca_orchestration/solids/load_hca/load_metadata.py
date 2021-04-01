from dagster import composite_solid, solid, InputDefinition, Nothing
from dagster.experimental import DynamicOutput, DynamicOutputDefinition


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
def query_rows_to_append(_context) -> Nothing:
    pass


@solid(
    input_defs=[InputDefinition("nothing", Nothing)]
)
def export_appends(_context) -> Nothing:
    pass


@solid(
    input_defs=[InputDefinition("nothing", Nothing)]
)
def ingest_metadata_to_jade(_context) -> Nothing:
    pass


@composite_solid(input_defs=[InputDefinition("table_name", str)])
def import_metadata(table_name: str) -> Nothing:
    return ingest_metadata_to_jade(
        export_appends(query_rows_to_append(diff_against_existing_data(table_name))))
