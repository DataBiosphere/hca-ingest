"""
Complex type signatures that appear multiple times throughout the code
base can live here, for easy reference and descriptive naming.
"""

from dagster import make_python_type_usable_as_dagster_type


class HcaScratchDatasetName(str):
    pass


make_python_type_usable_as_dagster_type(HcaStagingDatasetName, str)
