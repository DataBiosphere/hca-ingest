from dagster import PipelineDefinition, repository

from hca_orchestration.repositories.base_repositories import base_jobs


@repository
def all_jobs() -> list[PipelineDefinition]:
    return base_jobs("prod")
