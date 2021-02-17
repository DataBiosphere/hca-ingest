from dagster import pipeline, repository
from hca_proto.resources.modes import prod_mode, dev_mode, test_mode
from hca_proto.solids import clear_staging_dir, pre_process_metadata, submit_file_ingest


@pipeline(
    mode_defs=[prod_mode, dev_mode, test_mode]
)
def stage_data():
    middle = pre_process_metadata(clear_staging_dir())
    entities = ["analysis_file", "analysis_process", "analysis_protocol"]

    outs = []
    for e in entities:
        submit = submit_file_ingest.alias(e)
        outs.append(submit(middle))

    final = submit_file_ingest.alias("final")
    final(outs)


@repository
def hca_prototype():
    return [stage_data]
