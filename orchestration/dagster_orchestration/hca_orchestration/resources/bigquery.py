from google.cloud import bigquery

from dagster import resource
from dagster.core.execution.context.init import InitResourceContext


@resource
def bigquery_client(init_context: InitResourceContext) -> bigquery.client.Client:
    return bigquery.Client()
