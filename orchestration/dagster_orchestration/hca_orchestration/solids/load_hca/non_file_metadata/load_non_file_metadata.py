from enum import Enum
from typing import Iterator

from dagster import solid, composite_solid, Nothing
from dagster.experimental import DynamicOutput, DynamicOutputDefinition

from hca_orchestration.solids.load_hca.load_table import load_table
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult


class NonFileMetadataTypes(Enum):
    AGGREGATE_GENERATION_PROTOCOL = MetadataType("aggregate_generation_protocol")
    ANALYSIS_PROCESS = MetadataType("analysis_process")
    ANALYSIS_PROTOCOL = MetadataType("analysis_protocol")
    CELL_LINE = MetadataType("cell_line")
    CELL_SUSPENSION = MetadataType("cell_suspension")
    COLLECTION_PROTOCOL = MetadataType("collection_protocol")
    DIFFERENTIATION_PROTOCOL = MetadataType("differentiation_protocol")
    DISSOCIATION_PROTOCOL = MetadataType("dissociation_protocol")
    DONOR_ORGANISM = MetadataType("donor_organism")
    ENRICHMENT_PROTOCOL = MetadataType("enrichment_protocol")
    IMAGED_SPECIMEN = MetadataType("imaged_specimen")
    IMAGING_PREPARATION_PROTOCOL = MetadataType("imaging_preparation_protocol")
    IMAGING_PROTOCOL = MetadataType("imaging_protocol")
    IPSC_INDUCTION_PROTOCOL = MetadataType("ipsc_induction_protocol")
    LIBRARY_PREPARATION_PROTOCOL = MetadataType("library_preparation_protocol")
    ORGANOID = MetadataType("organoid")
    PROCESS = MetadataType("process")
    PROJECT = MetadataType("project")
    PROTOCOL = MetadataType("protocol")
    SEQUENCING_PROTOCOL = MetadataType("sequencing_protocol")
    SPECIMEN_FROM_ORGANISM = MetadataType("specimen_from_organism")
    LINKS = MetadataType("links")


@solid(
    output_defs=[
        DynamicOutputDefinition(name="nonfile_table_fanout_result", dagster_type=MetadataTypeFanoutResult)
    ]
)
def ingest_non_file_metadata_type(scratch_dataset_name: HcaScratchDatasetName) -> Iterator[MetadataTypeFanoutResult]:
    """
    For each metadata type, return a dynamic output over which we can later map
    This saves us from hardcoding solids for each metadata type
    """
    for non_file_metadata_type in NonFileMetadataTypes:
        yield DynamicOutput(
            value=MetadataTypeFanoutResult(scratch_dataset_name, non_file_metadata_type.value),
            mapping_key=non_file_metadata_type.value,
            output_name="nonfile_table_fanout_result"
        )


@composite_solid
def non_file_metadata_fanout(scratch_dataset_name: HcaScratchDatasetName) -> Nothing:
    ingest_non_file_metadata_type(scratch_dataset_name).map(load_table)
