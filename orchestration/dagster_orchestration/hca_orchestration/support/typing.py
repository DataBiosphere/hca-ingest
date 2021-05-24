"""
Complex type signatures that appear multiple times throughout the code
base can live here, for easy reference and descriptive naming.
"""

from typing import NamedTuple

from dagster import make_python_type_usable_as_dagster_type
from dagster.core.types.dagster_type import String as DagsterString


class HcaScratchDatasetName(str):
    pass


make_python_type_usable_as_dagster_type(HcaScratchDatasetName, DagsterString)


class MetadataType(str):
    pass


class MetadataTypeFanoutResult(NamedTuple):
    scratch_dataset_name: HcaScratchDatasetName
    metadata_type: MetadataType
    is_file_metadata: bool = False
