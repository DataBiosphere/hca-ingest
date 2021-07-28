from dataclasses import dataclass
from dagster import resource, InitResourceContext


@dataclass
class HcaProjectConfig:
    project_id: str


@resource({
    "project_id": str
})
def hca_project_config(context: InitResourceContext):
    return HcaProjectConfig(context.resource_config["project_id"])
