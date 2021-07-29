import data_repo_client
from dagster import ModeDefinition, pipeline, ResourceDefinition
from dagster_utils.resources.bigquery import bigquery_client, noop_bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources.config.hca_dataset import target_hca_dataset
from hca_orchestration.solids.copy_project.subgraph_hydration import hydrate_subgraphs
from hca_orchestration.solids.copy_project.tabular_data_ingestion import ingest_tabular_data
from hca_orchestration.solids.copy_project.data_file_copy import copy_data_files
from hca_orchestration.solids.copy_project.ingest_file_id import inject_file_ids
from hca_orchestration.solids.copy_project.data_file_ingestion import ingest_data_files
from hca_orchestration.resources.hca_project_config import hca_project_config
from hca_orchestration.resources.snaphot_config import snapshot_config
from hca_orchestration.resources.scratch_config import scratch_config
from hca_orchestration.resources import bigquery_service

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "bigquery_client": bigquery_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "scratch_config": scratch_config,
        "bigquery_service": bigquery_service,
        "snapshot_config": snapshot_config,
        "hca_project_config": hca_project_config,
        "target_hca_dataset": target_hca_dataset
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
        "snapshot_config": snapshot_config,
        "hca_project_config": hca_project_config,
        "target_hca_dataset": target_hca_dataset
    }
)


@pipeline(
    mode_defs=[test_mode, dev_mode]
)
def copy_project() -> None:
    inject_file_ids(
        ingest_data_files(
            ingest_tabular_data(
                hydrate_subgraphs()
            )
        )
    )
