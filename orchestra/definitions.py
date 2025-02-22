from dagster import Definitions

from orchestra.assets.hca_assets import hca_project_metadata, hca_staging_data
from orchestra.jobs.ingestion import execute_data_ingestion
from orchestra.jobs.snapshot import execute_full_snapshot_workflow
from orchestra.jobs.validation import validate_ingress_job, pre_flight_validation_job
from orchestra.resources.config import resources

defs = Definitions(
    jobs=[
        pre_flight_validation_job,
        execute_data_ingestion,
        execute_full_snapshot_workflow,
        validate_ingress_job,
    ],
    assets=[hca_project_metadata, hca_staging_data],
    resources=resources,
)
