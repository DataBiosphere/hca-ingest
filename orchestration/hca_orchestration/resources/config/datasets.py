import json
import uuid
from datetime import datetime
from typing import Optional

from dagster import Array, Field, InitResourceContext, Noneable, resource

from hca_orchestration.contrib.data_repo.data_repo_service import (
    DataRepoService,
)
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.support.dates import dataset_snapshot_formatted_date


@resource(
    required_resource_keys={"data_repo_service"},
    config_schema={
        "dataset_id": str
    }
)
def passthrough_hca_dataset(init_context: InitResourceContext) -> TdrDataset:
    """
    Given a TDR dataset UUID, fetches the dataset info from TDR and returns
    a fully hydrated TdrDataset object.
    """
    data_repo_service: DataRepoService = init_context.resources.data_repo_service
    target_dataset = data_repo_service.get_dataset(init_context.resource_config["dataset_id"])

    return target_dataset


@resource(
    config_schema={
        "env": str,
        "region": str,
        "policy_members": Array(str),
        "billing_profile_id": str,
        "atlas": str,
        "qualifier": Field(Noneable(str), default_value=None, is_required=False),
    },
    required_resource_keys={"hca_project_id", "data_repo_service", "run_start_time"}
)
def find_or_create_project_dataset(init_context: InitResourceContext) -> Optional[TdrDataset]:
    """
    Given a source HCA project ID (via the required resource key `hca_project_id`) and an optional qualifier,
    searches TDR for a dataset matching those filtering parameters. If none is found, creates a new one following
    the DCP system naming scheme.

    Fails if more than one candidate dataset if found.
    """
    hca_project_id = uuid.UUID(init_context.resources.hca_project_id)
    qualifier = init_context.resource_config['qualifier']
    env = init_context.resource_config['env']
    atlas = init_context.resource_config['atlas']
    env_prefix = "prod" if env == "real_prod" else env

    target_hca_dataset_prefix = f"{atlas}_{env_prefix}_{hca_project_id.hex.replace('-', '')}"
    run_start_time = init_context.resources.run_start_time
    creation_date = dataset_snapshot_formatted_date(datetime.utcfromtimestamp(run_start_time))

    # attempt to find an existing dataset
    init_context.log.info(f"Checking for existing dataset with prefix = {target_hca_dataset_prefix}")
    data_repo_service: DataRepoService = init_context.resources.data_repo_service
    result = data_repo_service.find_dataset(target_hca_dataset_prefix, qualifier)

    if not result:
        # no dataset found, create one with the qualifier appended if need be
        init_context.log.info(f"Target dataset with prefix {target_hca_dataset_prefix} not found, creating")
        target_hca_dataset_name = f"{target_hca_dataset_prefix}__{creation_date}"
        if qualifier:
            target_hca_dataset_name = f"{target_hca_dataset_name}_{qualifier}"

        dataset_metadata = {
            "hca_project_id": hca_project_id.hex
        }

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
