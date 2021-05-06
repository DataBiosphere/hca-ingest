from unittest.mock import Mock

from dagster import resource, InitResourceContext
from google.cloud.bigquery import Client
from hca_orchestration.contrib.google import get_credentials


@resource
def bigquery_client(init_context: InitResourceContext) -> Client:
    return Client(credentials=get_credentials())


@resource
def noop_bigquery_client(init_context: InitResourceContext) -> Client:
    return Mock(spec=Client)
