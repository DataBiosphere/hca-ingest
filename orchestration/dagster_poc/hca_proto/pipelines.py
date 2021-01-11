from dagster import pipeline, repository
from hca_proto.solids import clear_staging_dir, pre_process_metadata, dispatch_k8s_job


@pipeline
def stage_data():
    pre_process_metadata(clear_staging_dir())


@repository
def hca_proto():
    return [stage_data]