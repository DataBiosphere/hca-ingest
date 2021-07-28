from dagster import ModeDefinition, pipeline, ResourceDefinition

from hca_orchestration.solids.copy_project.create_scratch_area import create_scratch_area
from hca_orchestration.solids.copy_project.subgraph_hydration import hydrate_subgraphs
from hca_orchestration.solids.copy_project.tabular_data_ingestion import ingest_tabular_data
from hca_orchestration.solids.copy_project.data_file_copy import copy_data_files
from hca_orchestration.solids.copy_project.ingest_file_id import inject_file_ids
from hca_orchestration.solids.copy_project.data_file_ingestion import ingest_data_files
from hca_orchestration.resources.hca_project_config import hca_project_config
from hca_orchestration.resources.snaphot_config import snapshot_config


test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "bigquery_client": ResourceDefinition.mock_resource(),
        "data_repo_client": ResourceDefinition.mock_resource(),
        "gcs": ResourceDefinition.mock_resource(),
        "scratch_config": ResourceDefinition.mock_resource(),
        "bigquery_service": ResourceDefinition.mock_resource(),
        "snapshot_config": snapshot_config,
        "hca_project_config": hca_project_config,
    }
)


@pipeline(
    mode_defs=[test_mode]
)
def copy_project() -> None:
    ingest_data_files(
        inject_file_ids(
            copy_data_files(
                ingest_tabular_data(
                    hydrate_subgraphs(
                        create_scratch_area())))))
