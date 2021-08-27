from dataclasses import dataclass

from dagster import resource, String, Bool
from dagster.core.execution.context.init import InitResourceContext


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
