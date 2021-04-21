from hca_manage.manage import HcaManage

from dagster.core.execution.context.compute import AbstractComputeExecutionContext


def hca_manage_from_solid_context(context: AbstractComputeExecutionContext) -> HcaManage:
    return HcaManage(
        environment=context.solid_config["gcp_env"],
        project=context.solid_config["google_project_name"],
        dataset=context.solid_config["dataset_name"],
        data_repo_client=context.resources.data_repo_client
    )
