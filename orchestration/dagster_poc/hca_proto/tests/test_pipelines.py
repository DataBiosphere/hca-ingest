import unittest

from dagster import execute_pipeline

from hca_proto.pipelines import stage_data


class PipelinesTestCase(unittest.TestCase):
    def test_stage_data(self):
        result = execute_pipeline(
            stage_data,
            mode='test',
            run_config={
                'solids': {
                    'clear_staging_dir': {
                        'config': {
                            'staging_bucket_name': 'example_staging_bucket_name',
                            'staging_blob_name': 'example_staging_blob_name'
                        }
                    },
                    'pre_process_metadata': {
                        'config': {
                            'input_prefix': 'example_input_prefix',
                            'output_prefix': 'example_output_prefix'
                        }
                    }
                }
            }
        )

        self.assertTrue(result.success)


if __name__ == '__main__':
    unittest.main()