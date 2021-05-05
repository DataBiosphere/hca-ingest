from google.cloud.bigquery import Dataset, Client, QueryJobConfig
from dagster import resource, InitResourceContext
from hca_orchestration.contrib.google import get_credentials

from unittest.mock import create_autospec


@resource
def bigquery_client(init_context: InitResourceContext) -> Client:
    return Client(credentials=get_credentials())


# class NoopBigQueryClient:
#     def create_dataset(self, dataset: Dataset) -> None:
#         pass
#
#     def query(self, query: str, job_config: QueryJobConfig, location: str):
#         pass


@resource
def noop_bigquery_client(init_context: InitResourceContext) -> Client:
    return create_autospec(Client)  # Mock(spec=Client) #NoopBigQueryClient()
