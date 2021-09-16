from dataclasses import dataclass
from datetime import datetime

from dagster import resource, String, Bool
from dagster.core.execution.context.init import InitResourceContext

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService


@dataclass
class SnapshotCreationConfig:
    dataset_name: str
    snapshot_name: str
    managed_access: bool


@resource({
    'dataset_name': String,
    'snapshot_name': String,
    'managed_access': Bool
})
def snapshot_creation_config(init_context: InitResourceContext) -> SnapshotCreationConfig:
    return SnapshotCreationConfig(**init_context.resource_config)


@resource(required_resource_keys={"data_repo_service"},
          config_schema={
    "source_hca_project_id": String,
    "managed_access": Bool
})
def dev_refresh_snapshot_creation_config(init_context: InitResourceContext) -> SnapshotCreationConfig:
    source_hca_project_id = init_context.resource_config["source_hca_project_id"]
    data_repo_service: DataRepoService = init_context.resources.data_repo_service

    # find the existing dataset, bail out if none are found
    source_hca_dataset_prefix = f"hca_dev_{source_hca_project_id.replace('-', '')}"
    result = data_repo_service.find_dataset(source_hca_dataset_prefix, "dev")
    if not result:
        raise Exception(f"No dataset for HCA project_id {source_hca_project_id} found")

    # craft a new snapshot name
    creation_date = datetime.now().strftime("%Y%m%d")
    snapshot_name = f"{result.dataset_name}_{creation_date}"

    return SnapshotCreationConfig(result.dataset_name, snapshot_name, False)


@dataclass
class HcaManageConfig:
    gcp_env: str
    google_project_name: str


@resource({
    'gcp_env': String,
    'google_project_name': String,
})
def hca_manage_config(init_context: InitResourceContext) -> HcaManageConfig:
    return HcaManageConfig(**init_context.resource_config)


@dataclass
class HcaDatasetOperationConfig(HcaManageConfig):
    dataset_name: str


@resource(
    {
        'dataset_name': String,
    },
    required_resource_keys={'hca_manage_config'}
)
def hca_dataset_operation_config(init_context: InitResourceContext) -> HcaDatasetOperationConfig:
    return HcaDatasetOperationConfig(
        gcp_env=init_context.resources.hca_manage_config.gcp_env,
        google_project_name=init_context.resources.hca_manage_config.google_project_name,
        dataset_name=init_context.resource_config['dataset_name'],
    )
