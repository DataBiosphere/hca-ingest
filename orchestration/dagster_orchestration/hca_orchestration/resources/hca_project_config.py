from dataclasses import dataclass
from dagster import resource, InitResourceContext


@dataclass
class HcaProjectCopyingConfig:
    source_hca_project_id: str
    source_snapshot_name: str
    source_bigquery_project_id: str
    load_tag: str


@resource({
    "source_hca_project_id": str,
    "source_snapshot_name": str,
    "source_bigquery_project_id": str,
    "load_tag": str
})
def hca_project_copying_config(context: InitResourceContext):
    return HcaProjectCopyingConfig(
        source_hca_project_id=context.resource_config["source_hca_project_id"],
        source_snapshot_name=context.resource_config["source_snapshot_name"],
        source_bigquery_project_id=context.resource_config["source_bigquery_project_id"],
        load_tag=context.resource_config["load_tag"]
    )
