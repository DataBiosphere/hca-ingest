from dagster import job, op

from orchestra.resources.gcp import GCPStorageResource, BigQueryResource
from orchestra.resources.terra import TerraDataRepoResource


@op(required_resource_keys={"gcp_storage", "bigquery", "data_repo"})
def ingest_hca_data():
    pass


@job(resource_defs={"gcp_storage": GCPStorageResource(), "bigquery": BigQueryResource(),
                    "data_repo": TerraDataRepoResource()})
def load_hca_job():
    ingest_hca_data()
