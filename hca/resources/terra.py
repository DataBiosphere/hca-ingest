from typing import ClassVar
from dagster import ConfigurableResource
from hca.contrib.terra.data_repo.client import (
    terra_data_repo_client,
    local_terra_data_repo_client,
)
from data_repo_client import RepositoryApi


class TerraDataRepoResource(ConfigurableResource):
    repo_client: ClassVar[RepositoryApi] = terra_data_repo_client


class LocalTerraDataRepoResource(ConfigurableResource):
    repo_client: ClassVar[RepositoryApi] = local_terra_data_repo_client
