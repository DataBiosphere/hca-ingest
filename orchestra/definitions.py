from dagster import Definitions

from orchestra.assets.hca_assets import hca_project_metadata, hca_staging_data
from orchestra.jobs.data_ingestion import load_hca_job
from orchestra.jobs.snapshot_management import manage_snapshots_job
from orchestra.jobs.validation import validate_ingress_job
from orchestra.resources.config import resources

defs = Definitions(
    jobs=[load_hca_job, validate_ingress_job, manage_snapshots_job],
    assets=[hca_project_metadata, hca_staging_data],
    resources=resources
)
