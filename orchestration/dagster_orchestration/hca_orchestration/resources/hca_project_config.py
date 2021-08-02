from dataclasses import dataclass
from dagster import resource, InitResourceContext


@dataclass
class HcaProjectCopyingConfig:
    project_id: str


@resource({
    "project_id": str
})
def hca_project_copying_config(context: InitResourceContext):
    return HcaProjectCopyingConfig(context.resource_config["project_id"])
