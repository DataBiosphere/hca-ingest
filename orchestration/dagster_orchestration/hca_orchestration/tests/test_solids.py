import unittest

from dagster import execute_solid

from hca_orchestration.solids import post_import_validate


class SolidsTestCase(unittest.TestCase):
    def test_post_import_validate(self):
        """
        TODO
        """
        result = execute_solid(post_import_validate, input_values={"google_project_name": "broad-jade-dev-data", "dataset_name": "hca_dev_20201217_test4"})
        self.assertTrue(result.success)
