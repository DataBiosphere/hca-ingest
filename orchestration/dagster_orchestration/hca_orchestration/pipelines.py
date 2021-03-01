from dagster import pipeline, repository

from hca_orchestration.resources.modes import prod_mode, local_mode, test_mode
from hca_orchestration.solids import clear_staging_dir, pre_process_metadata, submit_file_ingest, post_import_validate


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def stage_data():
    middle = pre_process_metadata(start=clear_staging_dir())
    entities = ["analysis_file", "analysis_process", "analysis_protocol"]

    outs = []
    for e in entities:
        submit = submit_file_ingest.alias(e)
        outs.append(submit(middle))

    final = submit_file_ingest.alias("final")
    final(outs)


@repository
def hca_orchestrationtype():
    return [stage_data]


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def validate_egress():
    post_import_validate()
