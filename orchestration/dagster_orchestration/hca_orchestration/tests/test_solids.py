import unittest
from unittest.mock import Mock, patch

from dagster import execute_solid

from hca_orchestration.solids import post_import_validate


class SolidsTestCase(unittest.TestCase):
    @patch("hca_utils.utils.HcaUtils.get_all_table_names")
    @patch("hca_utils.utils.HcaUtils.get_duplicates")
    @patch("hca_utils.utils.HcaUtils.get_file_table_names")
    @patch("hca_utils.utils.HcaUtils.get_null_filerefs")
    def test_post_import_validate(self, mock_all_table_names: Mock, mock_duplicates: Mock, mock_file_table_names: Mock,
                                  mock_null_filerefs: Mock):
        """
        Mock bigQuery interactions and make sure the post_import_validate solid works as desired.
        """

        fake_table_names = {"fake", "names"}
        fake_duplicate_ids = {"fake", "ids"}
        fake_file_table_names = {"fake"}
        fake_null_fileref_ids = {"fake", "file", "ids"}

        mock_all_table_names.return_value = fake_table_names
        mock_duplicates.return_value = fake_duplicate_ids
        mock_file_table_names.return_value = fake_file_table_names
        mock_null_filerefs.return_value = fake_null_fileref_ids

        result = execute_solid(post_import_validate,
                               input_values={"google_project_name": "broad-jade-dev-data",
                                             "dataset_name": "hca_dev_20201217_test4"},
                               run_config={"solids": {"post_import_validate": {"config": {"gcp_env": "dev"}}}})
        self.assertTrue(result.success)
        expected_issues = len(fake_table_names) * len(fake_duplicate_ids) + len(fake_file_table_names) * len(
            fake_null_fileref_ids)
        self.assertEqual(expected_issues, result.output_value())
