from dagster import composite_solid, solid, InputDefinition, Nothing, OutputDefinition
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from hca_orchestration.pipelines import load_hca


@solid(
    input_defs=[InputDefinition("nothing", Nothing)],
    output_defs=[DynamicOutputDefinition(str)]
)
def fan_out_to_tables(_) -> str:
    for table in load_hca.HCA_TABLES:
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
    input_defs=[InputDefinition("nothing", Nothing)],
    output_defs=[OutputDefinition(name="fake_result", dagster_type=int)]
)
def ingest_metadata_to_jade(_context) -> Nothing:
    return 0


@composite_solid(
    input_defs=[InputDefinition("table_name", str)],
    output_defs=[OutputDefinition(name="fake_result", dagster_type=int)]
)
def import_metadata(table_name: str) -> int:
    return ingest_metadata_to_jade(
        export_appends(query_rows_to_append(diff_against_existing_data(table_name))))
