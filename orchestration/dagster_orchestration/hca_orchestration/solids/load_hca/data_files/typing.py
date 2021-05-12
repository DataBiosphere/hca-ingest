from typing import NamedTuple

from hca_orchestration.support.typing import HcaScratchDatasetName


class FileMetadataType(str):
    pass


class FileMetadataTypeFanoutResult(NamedTuple):
    scratch_dataset_name: HcaScratchDatasetName
    file_metadata_type: FileMetadataType
