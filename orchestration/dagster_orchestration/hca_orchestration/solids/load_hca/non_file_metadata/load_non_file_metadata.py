from enum import Enum
from typing import Iterator

from dagster import solid, composite_solid, Nothing
from dagster.experimental import DynamicOutput, DynamicOutputDefinition

from hca_orchestration.solids.load_hca.load_table import load_table
from hca_orchestration.solids.load_hca.non_file_metadata.typing import NonFileMetadataType, \
    NonFileMetadataTypeFanoutResult
from hca_orchestration.support.typing import HcaScratchDatasetName


class NonFileMetadataTypes(Enum):
    AGGREGATE_GENERATION_PROTOCOL = NonFileMetadataType("aggregate_generation_protocol")
    ANALYSIS_PROCESS = NonFileMetadataType("analysis_process")
    ANALYSIS_PROTOCOL = NonFileMetadataType("analysis_protocol")
    CELL_LINE = NonFileMetadataType("cell_line")
    CELL_SUSPENSION = NonFileMetadataType("cell_suspension")
    COLLECTION_PROTOCOL = NonFileMetadataType("collection_protocol")
    DIFFERENTIATION_PROTOCOL = NonFileMetadataType("differentiation_protocol")
    DISSOCIATION_PROTOCOL = NonFileMetadataType("dissociation_protocol")
    DONOR_ORGANISM = NonFileMetadataType("donor_organism")
    ENRICHMENT_PROTOCOL = NonFileMetadataType("enrichment_protocol")
    IMAGED_SPECIMEN = NonFileMetadataType("imaged_specimen")
    IMAGING_PREPARATION_PROTOCOL = NonFileMetadataType("imaging_preparation_protocol")
    IMAGING_PROTOCOL = NonFileMetadataType("imaging_protocol")
    IPSC_INDUCTION_PROTOCOL = NonFileMetadataType("ipsc_induction_protocol")
    LIBRARY_PREPARATION_PROTOCOL = NonFileMetadataType("library_preparation_protocol")
    ORGANOID = NonFileMetadataType("organoid")
    PROCESS = NonFileMetadataType("process")
    PROJECT = NonFileMetadataType("project")
    PROTOCOL = NonFileMetadataType("protocol")
    SEQUENCING_PROTOCOL = NonFileMetadataType("sequencing_protocol")
    SPECIMEN_FROM_ORGANISM = NonFileMetadataType("specimen_from_organism")
    LINKS = NonFileMetadataType("links")


@solid(
    output_defs=[
        DynamicOutputDefinition(name="table_fanout_result", dagster_type=NonFileMetadataTypeFanoutResult)
    ]
)
def ingest_non_file_metadata_type(scratch_dataset_name: HcaScratchDatasetName) -> Iterator[NonFileMetadataTypeFanoutResult]:
    """
    For each metadata type, return a dynamic output over which we can later map
    This saves us from hardcoding solids for each metadata type
    """
    for non_file_metadata_type in NonFileMetadataTypes:
        yield DynamicOutput(
            value=NonFileMetadataTypeFanoutResult(scratch_dataset_name, non_file_metadata_type.value),
            mapping_key=non_file_metadata_type.value,
            output_name="table_fanout_result"
        )


@composite_solid
def non_file_metadata_fanout(scratch_dataset_name: HcaScratchDatasetName) -> Nothing:
    ingest_non_file_metadata_type(scratch_dataset_name).map(load_table)
