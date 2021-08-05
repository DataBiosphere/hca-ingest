from dataclasses import dataclass

from dagster_utils.contrib.google import gs_path_from_bucket_prefix


@dataclass
class ScratchConfig:
    scratch_bucket_name: str
    scratch_prefix_name: str
    scratch_bq_project: str
    scratch_dataset_prefix: str
    scratch_table_expiration_ms: int

    def scratch_area(self) -> str:
        return gs_path_from_bucket_prefix(self.scratch_bucket_name, self.scratch_prefix_name)
