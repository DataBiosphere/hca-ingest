from dagster import PipelineDefinition
from dagster_utils.resources.slack import live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines import validate_ingress
from hca_orchestration.repositories.base_repositories import base_jobs

def validate_ingress_job() -> PipelineDefinition:
    return validate_ingress.to_job(
        name="validate_ingress",
        resource_defs={
            "slack": preconfigure_resource_for_mode(live_slack_client, "prod")
        }
    )


def all_jobs() -> list[PipelineDefinition]:
    base_jobs + validate_ingress_job()