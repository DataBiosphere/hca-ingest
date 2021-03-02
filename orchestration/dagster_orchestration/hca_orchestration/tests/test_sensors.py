import unittest
from unittest.mock import patch

from argo.workflows.client import ArchivedWorkflowServiceApi
from argo.workflows.client.models import V1alpha1Arguments, V1alpha1Parameter, V1alpha1Workflow, V1alpha1WorkflowSpec,\
    V1alpha1WorkflowStatus, V1ObjectMeta

from hca_orchestration.contrib.argo_workflows import ExtendedArgoWorkflow, generate_argo_archived_workflows_client
from hca_orchestration.sensors import ArgoHcaImportCompletionSensor


# helper to turn a list into a generator to help mock generator functions
def generator(iterable):
    for obj in iterable:
        yield obj


# the argo workflows api produces this abominable nested set of classes for each workflow,
# so we build one from simple params here to keep our tests lean
def mock_argo_workflow(name, uid, status, params={}):
    return V1alpha1Workflow(
        metadata=V1ObjectMeta(
            name=name,
            uid=uid,
        ),
        spec=V1alpha1WorkflowSpec(
            arguments=V1alpha1Arguments(
                parameters=[
                    V1alpha1Parameter(name=k, value=v)
                    for k, v in params.items()
                ]
            )
        ),
        status=V1alpha1WorkflowStatus(
            phase=status
        )
    )


def extend_workflow(workflow: V1alpha1Workflow) -> ExtendedArgoWorkflow:
    return ExtendedArgoWorkflow(workflow, argo_url='https://nonexistentsite.test', access_token='token')


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
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Succeeded', {
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', {
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        with patch('hca_orchestration.contrib.argo_workflows.ArgoArchivedWorkflowsClientMixin.list_archived_workflows', return_value=generator(archived_workflows)):
            workflows = list(ArgoHcaImportCompletionSensor(argo_url='https://nonexistentsite.test', access_token='token').successful_hca_import_workflows())

            self.assertNotIn(extend_workflow(archived_workflows[0]), workflows)
            self.assertIn(extend_workflow(archived_workflows[1]), workflows)
            self.assertIn(extend_workflow(archived_workflows[2]), workflows)

    def test_successful_hca_import_workflows_only_includes_succeeded_workflows(self):
        archived_workflows = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', {
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', {
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        with patch('hca_orchestration.contrib.argo_workflows.ArgoArchivedWorkflowsClientMixin.list_archived_workflows', return_value=generator(archived_workflows)):
            workflows = list(ArgoHcaImportCompletionSensor(argo_url='https://nonexistentsite.test', access_token='token').successful_hca_import_workflows())

            self.assertIn(extend_workflow(archived_workflows[0]), workflows)
            self.assertNotIn(extend_workflow(archived_workflows[1]), workflows)
            self.assertIn(extend_workflow(archived_workflows[2]), workflows)

    def test_generate_run_request_uses_workflow_name_for_run_key(self):
        sensor = ArgoHcaImportCompletionSensor(argo_url='https://nonexistentsite.test', access_token='token')
        workflow = extend_workflow(mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded', {'data-repo-name': 'datarepo_snatasnet'}))
        with patch('hca_orchestration.contrib.argo_workflows.ExtendedArgoWorkflow.inflate', return_value=workflow):
            req = sensor.generate_run_request(workflow)

            self.assertEqual(req.run_key, 'import-hca-total-defg')

    def test_generate_run_request_inflates_workflow(self):
        sensor = ArgoHcaImportCompletionSensor(argo_url='https://nonexistentsite.test', access_token='token')
        workflow = extend_workflow(mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded', {'data-repo-name': 'datarepo_snatasnet'}))
        with patch('hca_orchestration.contrib.argo_workflows.ExtendedArgoWorkflow.inflate', return_value=workflow) as mocked_inflate:
            sensor.generate_run_request(workflow)
            mocked_inflate.assert_called_once()

    def test_generate_run_request_provides_correct_pipeline_params(self):
        sensor = ArgoHcaImportCompletionSensor(argo_url='https://nonexistentsite.test', access_token='token')
        workflow = extend_workflow(mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded', {'data-repo-name': 'datarepo_snatasnet'}))
        with patch('hca_orchestration.contrib.argo_workflows.ExtendedArgoWorkflow.inflate', return_value=workflow):
            req = sensor.generate_run_request(workflow)
            self.assertEqual(req.run_config['solids']['post_import_validate']['config']['dataset_name'], 'snatasnet')
