# Complex type signatures that appear multiple times throughout the code
# base can live here, for easy reference and descriptive naming.

from dagster import DagsterType, InputDefinition
from dagster.config import ConfigType as DagsterConfigType

from typing import Literal, TypedDict, Union


DagsterConfigDict = dict[
    str,
    Union[
        dict[
            Literal['env'],
            str
        ],
        str,
        int,
        float,
        bool
    ]
]

DagsterSolidConfigSchema = dict[str, DagsterConfigType]


class DagsterSolidConfig(TypedDict, total=False):
    required_resource_keys: set[str]
    input_defs: list[InputDefinition]
    config_schema: DagsterSolidConfigSchema


def wrap_as_dagster_type(base_type: type, description="") -> DagsterType:
    return DagsterType(
        name=f"Dagster{base_type.__name__}",
        type_check_fn=lambda _, value: isinstance(value, base_type),
        description=description
    )
