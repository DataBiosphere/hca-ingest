from enum import Enum

from dagster import SolidExecutionResult, execute_solid
from dagster_utils.contrib.data_repo.typing import JobId

from hca_orchestration.solids.load_hca.ingest_metadata_type import ingest_metadata_type
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType


class FakeEnum(Enum):
    FIRST = MetadataType("first")
    SECOND = MetadataType("second")
    THIRD = MetadataType("third")


FakeDatasetName = HcaScratchDatasetName("fake_dataset_name")


def test_fans_out_correctly():
    result: SolidExecutionResult = execute_solid(
        ingest_metadata_type,
        input_values={
            "scratch_dataset_name": FakeDatasetName
        },
        run_config={
            "solids": {
                "ingest_metadata_type": {
                    "config": {
                        "metadata_types": FakeEnum,
                        "prefix": "fakepath"
                    },
                    "inputs": {
                        "result": [JobId("abcdef")]
                    }
                }
            }
        }
    )

    assert result.success

    expected = [e.value for e in FakeEnum]
    for i, res in enumerate(result.output_value("table_fanout_result")):
        assert expected[i] == res
