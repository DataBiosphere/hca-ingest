from dagster import PipelineDefinition, repository

from hca_orchestration.config.dcp_release.dcp_release import load_dcp_release_manifests
from hca_orchestration.repositories.base_repositories import base_jobs


@repository
def all_jobs() -> list[PipelineDefinition]:
    jobs = base_jobs()
    jobs += load_dcp_release_manifests()
    return jobs
