from dataclasses import dataclass
from urllib.parse import urljoin

from dagster import resource, String
from dagster.core.execution.context.init import InitResourceContext


@dataclass
class DagitConfig:
    url: str

    def run_url(self, run_id: str) -> str:
        return urljoin(self.url, f"instance/runs/{run_id}")


@resource({
    'url': String,
})
def dagit_config(init_context: InitResourceContext) -> DagitConfig:
    return DagitConfig(**init_context.resource_config)
