from dagster import composite_solid, solid, InputDefinition, Nothing, OutputDefinition
from hca_orchestration.pipelines.utils import TableFanoutResult


@solid(
    input_defs=[InputDefinition("table_fanout_result", TableFanoutResult)]
)
def diff_against_existing_data(context, table_fanout_result: TableFanoutResult) -> Nothing:
    context.log.info(
        f"diff_against_existing_data; table_name = {table_fanout_result.table_name}; \
        staging_dataset = {table_fanout_result.staging_dataset}"
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


@composite_solid(
    input_defs=[InputDefinition("table_fanout_result", TableFanoutResult)]
)
def import_metadata(table_fanout_result: TableFanoutResult) -> Nothing:
    return ingest_metadata_to_jade(
        export_appends(query_rows_to_append(
            diff_against_existing_data(table_fanout_result)))
    )
