from typing import Iterator

from dagster import op, Failure, In, Nothing, AssetMaterialization, AssetKey, Out, Output
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.check import CheckManager
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.resources.hca_project_config import HcaProjectCopyingConfig


@op(
    required_resource_keys={
        "data_repo_client",
        "target_hca_dataset",
        "hca_project_copying_config",
    },
    ins={"start": In(Nothing)}
)
def validate_copied_dataset(context: AbstractComputeExecutionContext) -> Iterator[Output]:
    target_hca_dataset: TdrDataset = context.resources.target_hca_dataset
    hca_project_config: HcaProjectCopyingConfig = context.resources.hca_project_copying_config
    #
    # result = CheckManager(
    #     environment="dev",
    #     project=target_hca_dataset.project_id,
    #     dataset=target_hca_dataset.dataset_name,
    #     data_repo_client=context.resources.data_repo_client,
    #     snapshot=False
    # ).check_for_all()
    #
    # if result.has_problems():
    #     raise Failure(f"Dataset {target_hca_dataset.dataset_name} failed validation")

    yield AssetMaterialization(
        asset_key=AssetKey([hca_project_config.source_hca_project_id, target_hca_dataset.project_id,
                            target_hca_dataset.dataset_name, target_hca_dataset.dataset_id]),
        partition=f"{hca_project_config.source_hca_project_id}",
        tags={
            "dataset_id": target_hca_dataset.dataset_id,
            "project_id": target_hca_dataset.project_id,
            "dataset_name": target_hca_dataset.dataset_name,
            "source_hca_project_id": hca_project_config.source_hca_project_id
        }
    )

    yield Output(False)
