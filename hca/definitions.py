from dagster import Definitions

from hca.assets.hca_assets import hca_project_metadata, hca_staging_data
from hca.jobs.ingestion import execute_data_ingestion
from hca.jobs.snapshot import execute_full_snapshot_workflow
from hca.jobs.validation import pre_ingest_validation, post_ingest_validation
from hca.resources.config import resources

defs = Definitions(
    jobs=[
        pre_ingest_validation,
        execute_data_ingestion,
        execute_full_snapshot_workflow,
        post_ingest_validation,
    ],
    assets=[hca_project_metadata, hca_staging_data],
    resources=resources,
)
