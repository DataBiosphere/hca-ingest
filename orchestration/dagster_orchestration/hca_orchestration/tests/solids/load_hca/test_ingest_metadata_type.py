from enum import Enum
import unittest

from dagster import SolidExecutionResult, execute_solid

from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.ingest_metadata_type import ingest_metadata_type
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult


class FakeEnum(Enum):
    FIRST = MetadataType("first")
    SECOND = MetadataType("second")
    THIRD = MetadataType("third")


FakeDatasetName = HcaScratchDatasetName("fake_dataset_name")


class IngestMetadataTypeSolidTestCase(unittest.TestCase):
    def test_fans_out_correctly(self):
        result: SolidExecutionResult = execute_solid(
            ingest_metadata_type,
            mode_def=test_mode,
            input_values={
                "scratch_dataset_name": FakeDatasetName
            },
            run_config={
                "solids": {
                    "ingest_metadata_type": {
                        "config": {
                            "metadata_types": FakeEnum,
                            "path": "fakepath"
                        }
                    }
                }
            }
        )

        self.assertTrue(result.success)

        expected = [e.value for e in FakeEnum]
        for i, res in enumerate(result.output_value("table_fanout_result")):
            self.assertEqual(expected[i], res)
