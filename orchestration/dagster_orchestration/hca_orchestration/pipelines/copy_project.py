from dagster import ModeDefinition, pipeline, ResourceDefinition

from hca_orchestration.solids.copy_project.create_scratch_area import create_scratch_area
from hca_orchestration.solids.copy_project.subgraph_hydration import create_subgraph_hydration
from hca_orchestration.solids.copy_project.tabular_data_ingestion import create_tabular_ingestion
from hca_orchestration.solids.copy_project.data_file_copy import create_data_file_copy
from hca_orchestration.solids.copy_project.ingest_file_id import create_ingest_file_id
from hca_orchestration.solids.copy_project.data_file_ingestion import create_data_file_ingestion

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "bigquery_client": ResourceDefinition.mock_resource(),
        "data_repo_client": ResourceDefinition.mock_resource(),
        "gcs": ResourceDefinition.mock_resource(),
        "scratch_config": ResourceDefinition.mock_resource(),
        "bigquery_service": ResourceDefinition.mock_resource(),
        "snapshot_config": ResourceDefinition.mock_resource()
    }
)


@pipeline(
    mode_defs=[test_mode]
)
def copy_project():
    create_data_file_ingestion(create_ingest_file_id(create_data_file_copy(create_tabular_ingestion(create_subgraph_hydration(create_scratch_area())))))
