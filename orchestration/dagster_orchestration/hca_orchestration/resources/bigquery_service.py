from dagster import resource, InitResourceContext


from hca_orchestration.contrib.bigquery import BigQueryService

from unittest.mock import Mock


@resource(
    required_resource_keys={"bigquery_client"}
)
def mock_bigquery_service(init_context: InitResourceContext) -> BigQueryService:
    return Mock(spec=BigQueryService)


@resource(
    required_resource_keys={"bigquery_client"}
)
def bigquery_service(init_context: InitResourceContext) -> BigQueryService:
    return BigQueryService(init_context.resources.bigquery_client)
