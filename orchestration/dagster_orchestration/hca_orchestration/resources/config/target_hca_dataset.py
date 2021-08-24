import uuid
from datetime import datetime
from typing import Optional

from dagster import InitResourceContext, resource, String

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_manage.common import data_repo_profile_ids
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.resources.hca_project_config import HcaProjectCopyingConfig


@resource({
    "dataset_name": String,
    "dataset_id": String,
    "project_id": String,
    "billing_profile_id": String,
})
def target_hca_dataset(init_context: InitResourceContext) -> TdrDataset:
    return TdrDataset(**init_context.resource_config)


@resource(
    config_schema={
        "env": String
    },
    required_resource_keys={"hca_project_copying_config", "data_repo_service"}
)
def build_new_target_hca_dataset(init_context: InitResourceContext) -> Optional[TdrDataset]:
    hca_project_copying_config: HcaProjectCopyingConfig = init_context.resources.hca_project_copying_config
    hca_project_id = uuid.UUID(hca_project_copying_config.source_hca_project_id)

    creation_date = datetime.now().strftime("%Y%m%d")
    env = init_context.resource_config['env']
    target_hca_dataset_name = f"hca_{env}_{creation_date}_{hca_project_id.hex.replace('-', '')}"

    init_context.log.info(f"Checking for existing dataset named = {target_hca_dataset_name}")
    data_repo_service: DataRepoService = init_context.resources.data_repo_service
    result = data_repo_service.find_dataset(target_hca_dataset_name, env)

    if not result:
        init_context.log.info(f"Target dataset {target_hca_dataset_name} not found, creating")
        return data_repo_service.create_dataset(
            target_hca_dataset_name,
            env,
            data_repo_profile_ids[env],
            {"aherbst@broadinstitute.org"},
            "us-central1",
            f"Dataset for HCA project {hca_project_id}"
        )

    return result
