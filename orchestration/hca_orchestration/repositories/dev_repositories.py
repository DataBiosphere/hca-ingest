from dagster import PipelineDefinition, repository
from dagster_utils.resources.slack import live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines.validate_ingress import validate_ingress_graph, staging_area_validator
from hca_orchestration.repositories.base_repositories import base_jobs


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
    jobs = base_jobs()

    jobs.append(validate_ingress_job())
    return jobs
