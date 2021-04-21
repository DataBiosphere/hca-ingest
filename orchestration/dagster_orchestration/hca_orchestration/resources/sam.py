from urllib.parse import urljoin
from dataclasses import dataclass

from dagster import configured, Field, resource, StringSource
from dagster.core.execution.context.init import InitResourceContext
from google.auth.transport.requests import AuthorizedSession

from hca_orchestration.contrib.google import authorized_session
from hca_orchestration.support.typing import DagsterConfigDict


@dataclass
class Sam:
    base_url: str

    def make_snapshot_public(self, snapshot_id: str) -> None:
        response = self._session().put(
            self._api_url(f'api/resources/v1/datasnapshot/{snapshot_id}/policies/reader/public'),
            data="true",  # telling the endpoint to set the flag to true
        )

        # raise an exception for a bad response
        response.raise_for_status()

    # private methods

    def _api_url(self, url_part: str) -> str:
        return urljoin(self.base_url, url_part)

    def _session(self) -> AuthorizedSession:
        return authorized_session()


@resource({
    "api_url": Field(StringSource)
})
def sam(init_context: InitResourceContext) -> Sam:
    return Sam(base_url=init_context.resource_config['api_url'])


@configured(sam)
def prod_sam_client(_config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'api_url': {'env': 'SAM_URL'}
    }


class NoopSamClient:
    def make_snapshot_public(self, snapshot_id: str) -> None:
        pass


@resource
def noop_sam_client(init_context: InitResourceContext) -> NoopSamClient:
    return NoopSamClient()
