from typing import Iterator

from dagster import op, Failure, In, Nothing, AssetMaterialization, AssetKey, Out, Output
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.common import ProblemCount
from hca_orchestration.contrib.bigquery import BigQueryService
from hca_manage.check import CheckManager
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.resources.hca_project_config import HcaProjectCopyingConfig
from hca_manage.verify_subgraphs import verify_all_subgraphs_in_dataset


@op(
    required_resource_keys={
        "target_hca_dataset",
        "bigquery_service",
        "bigquery_client",
        "hca_project_copying_config"
    }
)
def verify_subgraphs(context: AbstractComputeExecutionContext, result: ProblemCount) -> None:
    bigquery_service: BigQueryService = context.resources.bigquery_service
    target_hca_dataset: TdrDataset = context.resources.target_hca_dataset
    project_copying_config: HcaProjectCopyingConfig = context.resources.hca_project_copying_config

    projects_found = bigquery_service.get_projects_in_dataset(
        target_hca_dataset.dataset_name,
        target_hca_dataset.project_id,
        target_hca_dataset.bq_location
    )

    if projects_found != {project_copying_config.source_hca_project_id}:
        raise Failure(
            f"Incorrect projects present in dataset {target_hca_dataset.dataset_name}, should be {project_copying_config.source_hca_project_id}, found {projects_found}")

    links_rows = bigquery_service.get_links_in_dataset(target_hca_dataset.dataset_name,
                                                       target_hca_dataset.project_id,
                                                       target_hca_dataset.bq_location)
    verify_all_subgraphs_in_dataset(
        links_rows,
        target_hca_dataset.project_id,
        target_hca_dataset.dataset_name,
        context.resources.bigquery_client)


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

    result = CheckManager(
        environment="dev",
        project=target_hca_dataset.project_id,
        dataset=target_hca_dataset.dataset_name,
        data_repo_client=context.resources.data_repo_client,
        snapshot=False
    ).check_for_all()

    if result.has_problems():
        raise Failure(f"Dataset {target_hca_dataset.dataset_name} failed validation")

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

    yield Output(result)
