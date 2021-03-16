import os

from dagster import configured, resource, StringSource, Field

from data_repo_client import ApiClient, Configuration, RepositoryApi

from hca_orchestration.contrib.google import default_google_access_token


@resource({
    "api_url": Field(StringSource)
})
def base_jade_data_repo_client(init_context):
    # create API client
    config = Configuration(host=init_context.resource_config["api_url"])
    config.access_token = default_google_access_token()
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    # submit file ingest (for now just enumerate datasets or something to prove interaction works)
    return RepositoryApi(api_client=client)


@configured(base_jade_data_repo_client)
def jade_data_repo_client(config):
    return {
        'api_url': os.environ.get('DATA_REPO_URL'),
    }


@resource
def noop_data_repo_client(init_context):
    class NoopDataRepoClient():
        class NoopResult():
            def __init__(self, total):
                self.total = total

        def enumerate_datasets(self):
            return NoopDataRepoClient.NoopResult(5)

    return NoopDataRepoClient()
