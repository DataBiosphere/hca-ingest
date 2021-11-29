import json
import uuid
from datetime import datetime
from typing import Optional

from dagster import InitResourceContext, resource, String, Array, Field, Noneable, Failure
from data_repo_client import DatasetModel

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.resources.hca_project_config import HcaProjectCopyingConfig


@resource(
    required_resource_keys={"data_repo_service"},
    config_schema={
        "dataset_id": String
    }
)
def target_hca_dataset(init_context: InitResourceContext) -> TdrDataset:
    data_repo_service: DataRepoService = init_context.resources.data_repo_service
    target_dataset = data_repo_service.get_dataset(init_context.resource_config["dataset_id"])

    return target_dataset


@resource(
    config_schema={
        "env": String,
        "region": String,
        "policy_members": Array(str),
        "billing_profile_id": String,
        "qualifier": Field(Noneable(str), default_value=None, is_required=False)
    },
    required_resource_keys={"hca_project_copying_config", "data_repo_service"}
)
def build_new_target_hca_dataset(init_context: InitResourceContext) -> Optional[TdrDataset]:
    hca_project_copying_config: HcaProjectCopyingConfig = init_context.resources.hca_project_copying_config
    hca_project_id = uuid.UUID(hca_project_copying_config.source_hca_project_id)

    qualifier = init_context.resource_config['qualifier']
    creation_date = datetime.now().strftime("%Y%m%d")
    env = init_context.resource_config['env']

    env_prefix = "prod" if env == "real_prod" else env
    target_hca_dataset_prefix = f"hca_{env_prefix}_{hca_project_id.hex.replace('-', '')}"
    target_hca_dataset_name = f"{target_hca_dataset_prefix}__{creation_date}"
    if qualifier:
        target_hca_dataset_name = f"{target_hca_dataset_prefix}_{qualifier}"

    init_context.log.info(f"Checking for existing dataset with prefix = {target_hca_dataset_prefix}")
    data_repo_service: DataRepoService = init_context.resources.data_repo_service
    datasets_list = data_repo_service.list_datasets(target_hca_dataset_prefix)

    # since the qualifer comes after the date in the HCA naming format, we need to do a two step match
    # 1. Find all datasets matching the prefix
    # 2. and if a qualifier is provided, search through those with a trailing _{qualifier}
    matching_datasets = []
    dataset: DatasetModel
    for dataset in datasets_list.items:
        if not qualifier:
            matching_datasets.append(dataset)
        elif dataset.name.endswith(qualifier):
            matching_datasets.append(dataset)

    if len(matching_datasets) > 1:
        raise Failure(f"More than one dataset found for project id {hca_project_id}")

    if matching_datasets:
        result = data_repo_service.get_dataset(matching_datasets[0].id)
    else:
        raise Failure(f"No matching dataset found for project id {hca_project_id}")

    dataset_metadata = {
        "hca_project_id": hca_project_id.hex
    }

    if not result:
        init_context.log.info(f"Target dataset named {target_hca_dataset_name} not found, creating")
        return data_repo_service.create_dataset(
            target_hca_dataset_name,
            env,
            init_context.resource_config["billing_profile_id"],
            init_context.resource_config["policy_members"],
            init_context.resource_config["region"],
            json.dumps(dataset_metadata)
        )
    else:
        init_context.log.info(f"Found existing target dataset [name = {result.dataset_name}, id = {result.dataset_id}]")

    return result
