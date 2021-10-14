"""
This is the primary ingest pipeline for HCA to TDR. It is responsible for extracting data from a staging
area, transforming via Google Cloud Dataflow jobs into a form suitable for ingestion to TDR and the final
load to TDR itself.
"""

from dagster import graph

from hca_orchestration.solids.load_hca.data_files.load_data_files import import_data_files
from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import file_metadata_fanout
from hca_orchestration.solids.load_hca.non_file_metadata.load_non_file_metadata import non_file_metadata_fanout
from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir, pre_process_metadata, create_scratch_dataset
from hca_orchestration.solids.load_hca.utilities import send_start_notification, validate_and_send_finish_notification


@graph
def load_hca() -> None:
    staging_dataset = create_scratch_dataset(
        pre_process_metadata(
            clear_scratch_dir(
                send_start_notification()
            )
        )
    )

    result = import_data_files(staging_dataset).collect()

    file_metadata_results = file_metadata_fanout(result, staging_dataset).collect()
    non_file_metadata_results = non_file_metadata_fanout(result, staging_dataset).collect()

    validate_and_send_finish_notification(file_metadata_results, non_file_metadata_results)
