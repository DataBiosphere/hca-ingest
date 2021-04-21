"""
Utilities for the hca_manage class can go here.

This file shouldn't last very long once we're done refactoring HcaManage.
After that, we should redesign it to be used as a resource.
"""
from hca_manage.manage import HcaManage

from dagster.core.execution.context.compute import AbstractComputeExecutionContext


def hca_manage_from_solid_context(context: AbstractComputeExecutionContext) -> HcaManage:
    return HcaManage(
        environment=context.solid_config["gcp_env"],
        project=context.solid_config["google_project_name"],
        dataset=context.solid_config["dataset_name"],
        data_repo_client=context.resources.data_repo_client
    )
