from urllib.parse import urljoin
from cached_property import cached_property
from dataclasses import dataclass

from dagster import configured, DagsterLogManager, Field, resource, String
from dagster.core.execution.context.init import InitResourceContext
from google.auth.transport.requests import AuthorizedSession

from hca_orchestration.contrib.google import authorized_session
from hca_orchestration.support.typing import DagsterConfigDict


@dataclass
class Sam:
    base_url: str

    def make_snapshot_public(self, snapshot_id: str) -> None:
        response = self._session.put(
            self._api_url(f'datasnapshot/{snapshot_id}/policies/reader/public'),
            data="true",  # telling the endpoint to set the flag to true
        )

        # raise an exception for a bad response
        response.raise_for_status()

    # private methods

    def _api_url(self, url_part: str) -> str:
        return urljoin(self.base_url, url_part)

    @cached_property
    def _session(self) -> AuthorizedSession:
        return authorized_session()


@dataclass
class NoopSam:
    logger: DagsterLogManager

    def make_snapshot_public(self, snapshot_id: str) -> None:
        self.logger.info(f"No-op request to make {snapshot_id} public triggered.")


@resource({
    "api_url": Field(String)
})
def sam(init_context: InitResourceContext) -> Sam:
    return Sam(base_url=init_context.resource_config['api_url'])


@configured(sam)
def prod_sam_client(_config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'api_url': 'https://sam.dsde-prod.broadinstitute.org/api/resources/v1'
    }


@resource
def noop_sam_client(init_context: InitResourceContext) -> NoopSam:
    return NoopSam(logger=init_context.log_manager)
