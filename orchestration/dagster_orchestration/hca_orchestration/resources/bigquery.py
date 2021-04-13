from google.cloud.bigquery import Dataset, Client
from dagster import resource, InitResourceContext
from hca_orchestration.contrib.google import get_credentials


@resource
def bigquery_client(init_context: InitResourceContext) -> Client:
    return Client(credentials=get_credentials())


class NoopBigQueryClient:
    def create_dataset(self, dataset: Dataset) -> None:
        pass


@resource
def noop_bigquery_client(init_context: InitResourceContext) -> Client:
    return NoopBigQueryClient()
