import unittest

from dagster import execute_solid, ModeDefinition, resource
from hca_proto.solids import clear_staging_dir, pre_process_metadata
from hca_proto.resources.test import test_beam_runner


class TestSolids(unittest.TestCase):
    def test_clear_staging_dir(self):
        @resource
        def mock_storage_client(init_context):
            class MockBlob:
                def delete(self):
                    pass

            class MockStorageClient:
                def list_blobs(self, bucket_name, prefix):
                    for i in range(0, 9):
                        yield MockBlob()

            return MockStorageClient()

        tmp_mode = ModeDefinition(name='tmp', resource_defs={
            'storage_client': mock_storage_client
        })
        result = execute_solid(
            clear_staging_dir,
            mode_def=tmp_mode,
            run_config={
                'solids': {
                    'clear_staging_dir': {
                        'config': {
                            'staging_bucket_name': '',
                            'staging_blob_name': ''
                        }
                    }
                }
            }
        )

        self.assertTrue(result.success)
        self.assertEqual(result.output_value(), 9)

    def test_preprocess_metadata(self):
        tmp_mode = ModeDefinition(name='tmp', resource_defs={
            'beam_runner': test_beam_runner
        })
        result = execute_solid(
            pre_process_metadata,
            mode_def=tmp_mode,
            run_config={
                'solids': {
                    'pre_process_metadata': {
                        'config': {
                            'input_prefix': 'foo',
                            'output_prefix': 'bar'
                        }
                    }
                }
            }
        )
        self.assertTrue(result.success)
