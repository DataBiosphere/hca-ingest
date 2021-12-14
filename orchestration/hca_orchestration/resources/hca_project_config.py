from dataclasses import dataclass
from dagster import resource, InitResourceContext


@dataclass
class HcaProjectCopyingConfig:
    source_hca_project_id: str
    source_snapshot_name: str
    source_bigquery_project_id: str
    source_bigquery_region: str


@resource(required_resource_keys={"hca_project_id"},
          config_schema={
    "source_snapshot_name": str,
    "source_bigquery_project_id": str,
    "source_bigquery_region": str
})
def hca_project_copying_config(context: InitResourceContext) -> HcaProjectCopyingConfig:
    return HcaProjectCopyingConfig(
        source_hca_project_id=context.resources.hca_project_id,
        source_snapshot_name=context.resource_config["source_snapshot_name"],
        source_bigquery_project_id=context.resource_config["source_bigquery_project_id"],
        source_bigquery_region=context.resource_config["source_bigquery_region"]
    )


@resource(config_schema={"hca_project_id": str})
def hca_project_id(init_context: InitResourceContext) -> str:
    return str(init_context.resource_config["hca_project_id"])
