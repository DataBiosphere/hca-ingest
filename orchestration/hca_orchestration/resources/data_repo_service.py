from dagster import resource, InitResourceContext

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService


@resource(
    required_resource_keys={"data_repo_client"}
)
def data_repo_service(init_context: InitResourceContext) -> DataRepoService:
    return DataRepoService(init_context.resources.data_repo_client)
