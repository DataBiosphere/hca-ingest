import unittest
from unittest.mock import Mock, patch

from dagster import execute_solid

from hca_manage.manage import ProblemCount

from hca_orchestration.pipelines.validate_egress import test_mode
from hca_orchestration.solids.validate_egress import base_post_import_validate,\
    base_notify_slack_of_egress_validation_results

from .support.matchers import StringContaining


class PostImportValidateTestCase(unittest.TestCase):
    @patch("hca_manage.manage.HcaManage.get_dangling_proj_refs", return_value=set())
    @patch("hca_manage.manage.HcaManage.get_null_filerefs", return_value=set())
    @patch("hca_manage.manage.HcaManage.get_file_table_names", return_value=set())
    @patch("hca_manage.manage.HcaManage.get_duplicates", return_value=set())
    @patch("hca_manage.manage.HcaManage.get_all_table_names", return_value=set())
    def test_post_import_validate(self, mock_all_table_names: Mock, mock_duplicates: Mock, mock_file_table_names: Mock,
                                  mock_null_filerefs: Mock, mock_dangling_proj_refs: Mock):
        """
        Mock bigQuery interactions and make sure the post_import_validate solid works as desired.
        """

        fake_table_names = {"duptable"}
        fake_duplicate_ids = {"dupid1", "dupid2"}
        fake_file_table_names = {"filetable"}
        fake_null_fileref_ids = {"nfrid1", "nfrid2", "nfrid3"}
        fake_dangling_proj_refs = {'fake1', 'fake2'}

        mock_all_table_names.return_value = fake_table_names
        mock_duplicates.return_value = fake_duplicate_ids
        mock_file_table_names.return_value = fake_file_table_names
        mock_null_filerefs.return_value = fake_null_fileref_ids
        mock_dangling_proj_refs.return_value = fake_dangling_proj_refs

        solid_config = {
            "solids": {
                "base_post_import_validate": {
                    "config": {
                        "gcp_env": "dev",
                        "google_project_name": "fakeproj",
                        "dataset_name": "fakedataset",
                    }
                }
            }
        }

        result = execute_solid(base_post_import_validate,
                               run_config=solid_config, mode_def=test_mode)
        self.assertTrue(result.success)
        expected_duplicate_issues = len(fake_table_names) * len(fake_duplicate_ids)
        expected_file_ref_issues = len(fake_file_table_names) * len(fake_null_fileref_ids)
        expected_dangling_proj_refs = len(fake_table_names) * len(fake_dangling_proj_refs)
        self.assertEqual(expected_duplicate_issues, result.output_value().duplicates)
        self.assertEqual(expected_file_ref_issues, result.output_value().null_file_refs)
        self.assertEqual(expected_dangling_proj_refs, result.output_value().dangling_project_refs)


class NotifySlackOfEgressValidationResultsTestCase(unittest.TestCase):
    def setUp(self):
        self.solid_config = {
            "solids": {
                "base_notify_slack_of_egress_validation_results": {
                    "config": {
                        "gcp_env": "dev",
                        "channel": "choonel",
                        "dataset_name": "fakedataset",
                    }
                }
            }
        }

    def test_notifies_slack_with_failure_info(self):
        with patch("hca_orchestration.resources.slack.ConsoleSlackClient.chat_postMessage") as slack_message_sender:
            result = execute_solid(
                base_notify_slack_of_egress_validation_results,
                run_config=self.solid_config,
                mode_def=test_mode,
                input_values={
                    "validation_results": ProblemCount(
                        duplicates=3, null_file_refs=2, dangling_project_refs=3
                    )
                }
            )

            expected_lines = [
                "Problems identified in post-validation for HCA dev dataset fakedataset",
                "Duplicate lines found: 3",
                "Null file references found: 2",
            ]

            for expected_line in expected_lines:
                slack_message_sender.assert_called_once_with(
                    channel="choonel",
                    text=StringContaining(expected_line)
                )

        self.assertTrue(result.success)

    def test_notifies_slack_of_success(self):
        with patch("hca_orchestration.resources.slack.ConsoleSlackClient.chat_postMessage") as slack_message_sender:
            result = execute_solid(
                base_notify_slack_of_egress_validation_results,
                run_config=self.solid_config,
                mode_def=test_mode,
                input_values={
                    "validation_results": ProblemCount(
                        duplicates=0, null_file_refs=0, dangling_project_refs=0
                    )
                }
            )

            slack_message_sender.assert_called_once_with(
                channel="choonel",
                text="HCA dev dataset fakedataset has passed post-validation."
            )

        self.assertTrue(result.success)
