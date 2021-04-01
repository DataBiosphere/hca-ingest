from dagster import composite_solid, solid, InputDefinition, Nothing, String, OutputDefinition


@solid(
    input_defs=[InputDefinition("table_name", str)]
)
def diff_file_loads(_context, table_name) -> Nothing:
    _context.log.info(
        f"diff_file_loads; table_name = {table_name}"
    )


@solid(
    input_defs=[InputDefinition("nothing", Nothing)]
)
def extract_file_loads(_context) -> Nothing:
    pass


@solid(
    input_defs=[InputDefinition("ignore", Nothing)]
)
def list_deduped_requests(_context) -> Nothing:
    pass


@solid(
    input_defs=[InputDefinition("nothing", Nothing)]
)
def run_bulk_file_ingest(_context) -> Nothing:
    pass


@solid(
    input_defs=[InputDefinition("nothing", Nothing)]
)
def ingest_file_metadata(_context) -> Nothing:
    pass


@composite_solid(
    input_defs=[InputDefinition("table_name", str)]
)
def import_data_files(table_name: str) -> Nothing:
    return ingest_file_metadata(run_bulk_file_ingest(
        list_deduped_requests(extract_file_loads(diff_file_loads(table_name)))))
