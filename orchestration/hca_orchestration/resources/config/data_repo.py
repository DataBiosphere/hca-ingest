import os
from dataclasses import dataclass
from datetime import datetime

from dagster import Bool, Noneable, String, resource
from dagster.core.execution.context.init import InitResourceContext

# isort: split

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.support.dates import dataset_snapshot_formatted_date


@dataclass
class SnapshotCreationConfig:
    dataset_name: str
    snapshot_name: str
    qualifier: str
    managed_access: bool


@resource(
    required_resource_keys={"run_start_time"},
    config_schema={
        'dataset_name': String,
        'qualifier': Noneable(String),
        'managed_access': Bool
    }
)
def snapshot_creation_config(init_context: InitResourceContext) -> SnapshotCreationConfig:
    # we use the pipeline start time instead of datetime.now() as this resource may be reconstructed at various
    # points during the pipeline run and therefore the time may change and lead to differing snapshot names

    run_start_time = init_context.resources.run_start_time
    dt_suffix = dataset_snapshot_formatted_date(datetime.utcfromtimestamp(run_start_time))
    snapshot_name = f"{init_context.resource_config['dataset_name']}___{dt_suffix}"

    qualifier = init_context.resource_config.get('qualifier', None)
    if qualifier:
        snapshot_name = f"{snapshot_name}_{qualifier}"

    return SnapshotCreationConfig(
        dataset_name=init_context.resource_config["dataset_name"],
        snapshot_name=snapshot_name,
        qualifier=qualifier,
        managed_access=init_context.resource_config["managed_access"]
    )


@resource(
    required_resource_keys={"data_repo_service"},
    config_schema={
        "source_hca_project_id": String,
        "qualifier": Noneable(String),
        "managed_access": Bool,
        "dataset_qualifier": Noneable(String),
        "atlas": String
    })
def project_snapshot_creation_config(init_context: InitResourceContext) -> SnapshotCreationConfig:
    source_hca_project_id = init_context.resource_config["source_hca_project_id"]
    data_repo_service: DataRepoService = init_context.resources.data_repo_service
    atlas = init_context.resource_config["atlas"]

    # find the existing dataset, bail out if none are found
    env = os.environ["ENV"]
    sanitized_hca_project_name = source_hca_project_id.replace('-', '')
    dataset_qualifier = init_context.resource_config.get('dataset_qualifier', None)
    source_hca_dataset_prefix = f"{atlas}_{env}_{sanitized_hca_project_name}"

    # we provide the dataset qualifier to allow distinction between dcp1/dcp2 datasets
    # (i.e., two datasets, same hca project ID but one is from dcp1 and the other from dcp2)
    result = data_repo_service.find_dataset(source_hca_dataset_prefix, qualifier=dataset_qualifier)
    if not result:
        raise Exception(f"No dataset for project_id {source_hca_project_id} found (qualifier={dataset_qualifier})")

    # craft a new snapshot name
    creation_date = datetime.now().strftime("%Y%m%d")
    dataset_suffix = result.dataset_name.split('__')[1].split('_')
    snapshot_suffix = dataset_suffix[0]
    if dataset_qualifier:
        dataset_qualifier = dataset_suffix[1]
        snapshot_suffix = f"{snapshot_suffix}_{dataset_qualifier}"

    snapshot_name = f"{atlas}_{env}_{sanitized_hca_project_name}__{snapshot_suffix}_{creation_date}"

    snapshot_qualifier = init_context.resource_config.get('qualifier', None)
    if snapshot_qualifier:
        snapshot_name = f"{snapshot_name}_{snapshot_qualifier}"

    return SnapshotCreationConfig(
        result.dataset_name,
        snapshot_name,
        snapshot_qualifier,
        init_context.resource_config["managed_access"])


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
