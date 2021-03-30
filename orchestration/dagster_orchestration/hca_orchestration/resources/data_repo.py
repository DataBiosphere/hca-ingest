from dataclasses import dataclass

from dagster import configured, resource, StringSource, Field
from dagster.core.execution.context.init import InitResourceContext

from data_repo_client import ApiClient, Configuration, RepositoryApi

from hca_orchestration.contrib.google import default_google_access_token
from hca_orchestration.support.typing import DagsterConfigDict


@resource({
    "api_url": Field(StringSource)
})
def base_jade_data_repo_client(init_context: InitResourceContext) -> RepositoryApi:
    # create API client
    config = Configuration(host=init_context.resource_config["api_url"])
    config.access_token = default_google_access_token()
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    # submit file ingest (for now just enumerate datasets or something to prove interaction works)
    return RepositoryApi(api_client=client)


@configured(base_jade_data_repo_client)
def jade_data_repo_client(_config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'api_url': {'env': 'DATA_REPO_URL'},
    }


class NoopDataRepoClient:
    @dataclass
    class NoopResult:
        total: int

    def enumerate_datasets(self) -> NoopResult:
        return NoopDataRepoClient.NoopResult(5)


@resource
def noop_data_repo_client(init_context: InitResourceContext) -> NoopDataRepoClient:
    return NoopDataRepoClient()
