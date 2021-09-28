from dagster import PipelineDefinition, repository
from dagster_utils.resources.slack import live_slack_client

from hca_orchestration.config.dev_refresh.dev_refresh import dev_refresh_cut_snapshot_partition_set, \
    copy_project_to_new_dataset_partitions
from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines.validate_ingress import validate_ingress_graph, staging_area_validator
from hca_orchestration.repositories.base_repositories import base_jobs, copy_project_to_new_dataset_job


def validate_ingress_job() -> PipelineDefinition:
    return validate_ingress_graph.to_job(
        name="validate_ingress",
        resource_defs={
            "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
            "staging_area_validator": staging_area_validator
        }
    )


@repository
def all_jobs() -> list[PipelineDefinition]:
    jobs = base_jobs("dev")

    jobs.append(validate_ingress_job())
    jobs += copy_project_to_new_dataset_partitions()
    jobs += dev_refresh_cut_snapshot_partition_set()
    jobs += copy_project_to_new_dataset_job(),
    return jobs
