import unittest
from unittest.mock import Mock, patch

from dagster import execute_solid

from hca_orchestration.pipelines.validate_egress import test_mode
from hca_orchestration.solids import post_import_validate


class SolidsTestCase(unittest.TestCase):
    @patch("hca_manage.manage.HcaManage.get_null_filerefs", return_value=set())
    @patch("hca_manage.manage.HcaManage.get_file_table_names", return_value=set())
    @patch("hca_manage.manage.HcaManage.get_duplicates", return_value=set())
    @patch("hca_manage.manage.HcaManage.get_all_table_names", return_value=set())
    def test_post_import_validate(self, mock_all_table_names: Mock, mock_duplicates: Mock, mock_file_table_names: Mock,
                                  mock_null_filerefs: Mock):
        """
        Mock bigQuery interactions and make sure the post_import_validate solid works as desired.
        """

        fake_table_names = {"duptable"}
        fake_duplicate_ids = {"dupid1", "dupid2"}
        fake_file_table_names = {"filetable"}
        fake_null_fileref_ids = {"nfrid1", "nfrid2", "nfrid3"}

        mock_all_table_names.return_value = fake_table_names
        mock_duplicates.return_value = fake_duplicate_ids
        mock_file_table_names.return_value = fake_file_table_names
        mock_null_filerefs.return_value = fake_null_fileref_ids

        solid_config = {
            "solids": {
                "post_import_validate": {
                    "config": {
                        "gcp_env": "dev",
                        "google_project_name": "fakeproj",
                        "dataset_name": "fakedataset",
                    }
                }
            }
        }

        result = execute_solid(post_import_validate, run_config=solid_config, mode_def=test_mode)
        self.assertTrue(result.success)
        expected_duplicate_issues = len(fake_table_names) * len(fake_duplicate_ids)
        expected_file_ref_issues = len(fake_file_table_names) * len(fake_null_fileref_ids)
        self.assertEqual(expected_duplicate_issues, result.output_value().duplicates)
        self.assertEqual(expected_file_ref_issues, result.output_value().null_file_refs)
