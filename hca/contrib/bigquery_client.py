from unittest.mock import Mock

from dagster import resource, InitResourceContext
from google.cloud.bigquery import Client

from hca.contrib.google import authorized_session


@resource
def bigquery_client(init_context: InitResourceContext) -> Client:
    return Client(_http=authorized_session())


@resource
def local_bigquery_client(init_context: InitResourceContext) -> Client:
    return Mock(spec=Client)
