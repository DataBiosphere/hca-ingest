"""
This is the primary ingest pipeline for HCA to TDR. It is responsible for extracting data from a staging
area, transforming via Google Cloud Dataflow jobs into a form suitable for ingestion to TDR and the final
load to TDR itself.
"""
import os
import warnings

import sentry_sdk
from dagster import ExperimentalWarning, graph

from hca_orchestration.solids.load_hca.data_files.load_data_files import (
    import_data_files,
)
from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import (
    file_metadata_fanout,
)
from hca_orchestration.solids.load_hca.non_file_metadata.load_non_file_metadata import (
    non_file_metadata_fanout,
)
from hca_orchestration.solids.load_hca.stage_data import (
    clear_scratch_dir,
    create_scratch_dataset, 
    pre_process_metadata,
)
from hca_orchestration.solids.load_hca.utilities import (
    send_start_notification, 
    validate_and_send_finish_notification,
)

warnings.filterwarnings("ignore", category=ExperimentalWarning)


SENTRY_DSN = os.getenv(
    "SENTRY_DSN",
    "",
)
if SENTRY_DSN:
    sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=1.0)

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
