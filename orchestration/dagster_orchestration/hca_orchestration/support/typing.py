# Complex type signatures that appear multiple times throughout the code base can live here,
# for easy reference and descriptive naming.

from typing import Union


DagsterConfigDict = dict[str, Union[dict[str, str], str]]
