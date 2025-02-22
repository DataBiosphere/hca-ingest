from dagster import job, op

from orchestra.resources.terra import TerraDataRepoResource


@op(required_resource_keys={"data_repo"})
def create_project_snapshot():
    pass


@job(resource_defs={"data_repo": TerraDataRepoResource()})
def manage_snapshots_job():
    create_project_snapshot()
