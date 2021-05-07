import unittest
from unittest.mock import patch

from datetime import datetime
from dateutil.tz import tzlocal

from argo.workflows.client import ArchivedWorkflowServiceApi
from dagster_utils.contrib.argo_workflows import generate_argo_archived_workflows_client
from dagster_utils.tests.support.mock_workflows import mock_argo_workflow, extend_workflow

from hca_orchestration.sensors import ArgoHcaImportCompletionSensor


class TestArgoWorkflowsClient(unittest.TestCase):
    def test_generates_a_client_with_the_specified_parameters(self):
        client = generate_argo_archived_workflows_client("https://zombo.com", "tokentokentoken")

        self.assertIsInstance(client, ArchivedWorkflowServiceApi)
        self.assertIn("Authorization", client.api_client.default_headers)
        self.assertEqual(client.api_client.default_headers['Authorization'], "Bearer tokentokentoken")
        self.assertEqual(client.api_client.configuration.host, "https://zombo.com")


class TestArgoHcaImportCompletionSensor(unittest.TestCase):
    def test_successful_hca_import_workflows_only_includes_import_workflows(self):
        archived_workflows = [
            mock_argo_workflow('abc-not-an-import', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        with patch('dagster_utils.contrib.argo_workflows.ArgoArchivedWorkflowsClient.list_archived_workflows',
                   return_value=archived_workflows):
            workflows = list(
                ArgoHcaImportCompletionSensor(
                    argo_url='https://nonexistentsite.test',
                    access_token='token'
                ).successful_hca_import_workflows()
            )

            self.assertNotIn(extend_workflow(archived_workflows[0]), workflows)
            self.assertIn(extend_workflow(archived_workflows[1]), workflows)
            self.assertIn(extend_workflow(archived_workflows[2]), workflows)

    def test_successful_hca_import_workflows_only_includes_succeeded_workflows(self):
        archived_workflows = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        with patch('dagster_utils.contrib.argo_workflows.ArgoArchivedWorkflowsClient.list_archived_workflows',
                   return_value=archived_workflows):
            workflows = list(
                ArgoHcaImportCompletionSensor(
                    argo_url='https://nonexistentsite.test',
                    access_token='token'
                ).successful_hca_import_workflows()
            )

            self.assertIn(extend_workflow(archived_workflows[0]), workflows)
            self.assertNotIn(extend_workflow(archived_workflows[1]), workflows)
            self.assertIn(extend_workflow(archived_workflows[2]), workflows)

    def test_successful_hca_import_workflows_ignores_workflows_finished_before_epoch(self):
        archived_workflows = [
            mock_argo_workflow(
                'import-hca-total-defg',
                'abc123uid',
                'Succeeded',
                datetime(2021, 3, 17, tzinfo=tzlocal()),
                {'data-repo-name': 'datarepo_dataset0'},
            ),
            mock_argo_workflow(
                'import-hca-total-abcd',
                'abc234uid',
                'Succeeded',
                datetime(2021, 3, 16, tzinfo=tzlocal()),
                {'data-repo-name': 'datarepo_dataset1'},
            ),
            mock_argo_workflow(
                'import-hca-total-cdef',
                'abc345uid',
                'Succeeded',
                datetime(2020, 2, 27, tzinfo=tzlocal()),
                {'data-repo-name': 'datarepo_dataset2'},
            ),
        ]

        with patch('dagster_utils.contrib.argo_workflows.ArgoArchivedWorkflowsClient.list_archived_workflows',
                   return_value=archived_workflows):
            workflows = list(
                ArgoHcaImportCompletionSensor(
                    argo_url='https://nonexistentsite.test',
                    access_token='token'
                ).successful_hca_import_workflows()
            )

            self.assertIn(extend_workflow(archived_workflows[0]), workflows)
            self.assertIn(extend_workflow(archived_workflows[1]), workflows)
            self.assertNotIn(extend_workflow(archived_workflows[2]), workflows)

    def test_generate_run_request_uses_workflow_name_for_run_key(self):
        sensor = ArgoHcaImportCompletionSensor(argo_url='https://nonexistentsite.test', access_token='token')
        workflow = extend_workflow(mock_argo_workflow(
            'import-hca-total-defg',
            'abc123uid',
            'Succeeded',
            params={'data-repo-name': 'datarepo_snatasnet'}
        ))
        with patch('dagster_utils.contrib.argo_workflows.ExtendedArgoWorkflow.inflate', return_value=workflow):
            req = sensor.generate_run_request(workflow)

            self.assertEqual(req.run_key, 'import-hca-total-defg')

    def test_generate_run_request_inflates_workflow(self):
        sensor = ArgoHcaImportCompletionSensor(argo_url='https://nonexistentsite.test', access_token='token')
        workflow = extend_workflow(mock_argo_workflow(
            'import-hca-total-defg',
            'abc123uid',
            'Succeeded',
            params={'data-repo-name': 'datarepo_snatasnet'}
        ))
        with patch('dagster_utils.contrib.argo_workflows.ExtendedArgoWorkflow.inflate',
                   return_value=workflow) as mocked_inflate:
            sensor.generate_run_request(workflow)
            mocked_inflate.assert_called_once()

    def test_generate_run_request_provides_correct_pipeline_params(self):
        sensor = ArgoHcaImportCompletionSensor(argo_url='https://nonexistentsite.test', access_token='token')
        workflow = extend_workflow(mock_argo_workflow(
            'import-hca-total-defg',
            'abc123uid',
            'Succeeded',
            params={'data-repo-name': 'datarepo_snatasnet'}
        ))
        with patch('dagster_utils.contrib.argo_workflows.ExtendedArgoWorkflow.inflate', return_value=workflow):
            req = sensor.generate_run_request(workflow)
            self.assertEqual(req.run_config['resources']['hca_dataset_operation_config']
                             ['config']['dataset_name'], 'snatasnet')
