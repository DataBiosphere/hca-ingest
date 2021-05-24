from typing import NamedTuple

from hca_orchestration.support.typing import HcaScratchDatasetName


class NonFileMetadataType(str):
    pass


class NonFileMetadataTypeFanoutResult(NamedTuple):
    scratch_dataset_name: HcaScratchDatasetName
    non_file_metadata_type: NonFileMetadataType
