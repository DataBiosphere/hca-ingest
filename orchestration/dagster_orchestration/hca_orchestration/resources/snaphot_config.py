from dataclasses import dataclass
from dagster import resource, InitResourceContext


@dataclass
class SnapshotConfig:
    snapshot_name: str
    bigquery_project_id: str


@resource({
    "snapshot_name": str,
    "bigquery_project_id": str
})
def snapshot_config(context: InitResourceContext):
    return SnapshotConfig(
        context.resource_config["snapshot_name"],
        context.resource_config["bigquery_project_id"],
    )
