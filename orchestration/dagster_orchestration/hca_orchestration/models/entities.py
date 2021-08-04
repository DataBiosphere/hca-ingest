from dataclasses import dataclass

from hca_orchestration.support.typing import MetadataType


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
