from dagster import resource, InitResourceContext


from hca_orchestration.contrib.bigquery import BigQueryService

from unittest.mock import Mock


@resource(
    required_resource_keys={"bigquery_client"}
)
def mock_bigquery_service(init_context: InitResourceContext) -> BigQueryService:
    svc = Mock(spec=BigQueryService)
    svc.get_num_rows_in_table = Mock(return_value=0)
    return svc


@resource(
    required_resource_keys={"bigquery_client"}
)
def bigquery_service(init_context: InitResourceContext) -> BigQueryService:
    return BigQueryService(init_context.resources.bigquery_client)
