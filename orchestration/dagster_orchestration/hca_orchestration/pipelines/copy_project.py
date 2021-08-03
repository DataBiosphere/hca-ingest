from dagster import ModeDefinition, pipeline, ResourceDefinition
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources import bigquery_service, load_tag
from hca_orchestration.resources.config.hca_dataset import target_hca_dataset
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.hca_project_config import hca_project_copying_config
from hca_orchestration.solids.copy_project.data_file_ingestion import ingest_data_files
from hca_orchestration.solids.copy_project.delete_outdated_tabular_data import delete_outdated_tabular_data
from hca_orchestration.solids.copy_project.inject_file_ids import inject_file_ids
from hca_orchestration.solids.copy_project.subgraph_hydration import hydrate_subgraphs
from hca_orchestration.solids.copy_project.tabular_data_ingestion import ingest_tabular_data
from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "bigquery_client": bigquery_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "scratch_config": scratch_config,
        "bigquery_service": bigquery_service,
        "hca_project_copying_config": hca_project_copying_config,
        "target_hca_dataset": target_hca_dataset,
        "load_tag": load_tag
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "bigquery_client": ResourceDefinition.mock_resource(),
        "data_repo_client": ResourceDefinition.mock_resource(),
        "gcs": ResourceDefinition.mock_resource(),
        "scratch_config": scratch_config,
        "bigquery_service": ResourceDefinition.mock_resource(),
        "hca_project_copying_config": hca_project_copying_config,
        "target_hca_dataset": target_hca_dataset,
        "load_tag": load_tag
    }
)


@pipeline(
    mode_defs=[test_mode, dev_mode]
)
def copy_project() -> None:
    inject_file_ids(
        delete_outdated_tabular_data(
            ingest_tabular_data(
                ingest_data_files(
                    hydrate_subgraphs(
                        clear_scratch_dir()
                    )
                )
            )
        )
    )
