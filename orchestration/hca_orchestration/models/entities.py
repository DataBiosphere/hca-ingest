from dataclasses import dataclass

from google.cloud.bigquery import Row
import json
from hca_orchestration.support.typing import MetadataType


@dataclass
class Subgraph:
    links_id: str
    content: dict[str, object]

    def __post_init__(self) -> None:
        self.links = self.content['links']


def build_subgraph_from_links_row(row: Row) -> Subgraph:
    return Subgraph(row["links_id"], json.loads(row["content"]))


@dataclass(eq=True, frozen=True)
class DataFileEntity:
    """Represents an HCA data file"""
    path: str
    hca_file_path: str


@dataclass
class MetadataEntity:
    """Represents an HCA metadata entity"""
    entity_type: MetadataType
    entity_id: str
