from dagster import op, Failure, In, Nothing
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.check import CheckManager
from hca_orchestration.models.hca_dataset import TdrDataset


@op(
    required_resource_keys={"data_repo_client", "target_hca_dataset"},
    ins={"start": In(Nothing)}
)
def validate_copied_dataset(context: AbstractComputeExecutionContext) -> None:
    target_hca_dataset: TdrDataset = context.resources.target_hca_dataset
    result = CheckManager(
        environment="dev",
        project=target_hca_dataset.project_id,
        dataset=target_hca_dataset.dataset_name,
        data_repo_client=context.resources.data_repo_client
    ).check_for_all()

    if not result:
        raise Failure(f"Dataset {target_hca_dataset.dataset_name} failed validation")
