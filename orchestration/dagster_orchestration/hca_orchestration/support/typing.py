"""
Complex type signatures that appear multiple times throughout the code
base can live here, for easy reference and descriptive naming.
"""

from dagster import make_python_type_usable_as_dagster_type
from dagster.core.types.dagster_type import String as DagsterString



class HcaScratchDatasetName(str):
    pass


make_python_type_usable_as_dagster_type(HcaStagingDatasetName, DagsterString)
