from dagster import graph

from hca_orchestration.solids.copy_project.data_file_ingestion import ingest_data_files
from hca_orchestration.solids.copy_project.delete_outdated_tabular_data import delete_outdated_tabular_data
from hca_orchestration.solids.copy_project.inject_file_ids import inject_file_ids
from hca_orchestration.solids.copy_project.subgraph_hydration import build_subgraph_nodes, hydrate_subgraphs
from hca_orchestration.solids.copy_project.tabular_data_ingestion import ingest_tabular_data
from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir
from hca_orchestration.solids.validate_dataset import validate_copied_dataset


@graph
def copy_project() -> None:
    validate_copied_dataset(
        # delete_outdated_tabular_data(
        #     inject_file_ids(
        #         ingest_tabular_data(
        #             ingest_data_files(
        #                 hydrate_subgraphs(
        #                     clear_scratch_dir()
        #                 )
        #             )
        #         )
        #     )
        # )
    )
