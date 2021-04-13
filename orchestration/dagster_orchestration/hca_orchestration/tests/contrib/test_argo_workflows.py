import unittest

from unittest.mock import MagicMock, patch, call
from typing import Generator, TypeVar
from collections.abc import Iterable
from datetime import datetime
from dateutil.tz import tzlocal

from hca_orchestration.contrib.argo_workflows import ExtendedArgoWorkflow, generate_argo_archived_workflows_client, ArgoFetchListOperation, ArgoArchivedWorkflowsClient
from argo.workflows.client.models import V1alpha1Workflow, V1ObjectMeta, V1alpha1WorkflowSpec, V1alpha1Arguments, V1alpha1Parameter,\
    V1alpha1WorkflowStatus


# the argo workflows api produces this abominable nested set of classes for each workflow,
# so we build one from simple params here to keep our tests lean
def mock_argo_workflow(name, uid, status, finished_at=datetime.now(tz=tzlocal()), params={}):
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
            phase=status,
            finished_at=finished_at,
        )
    )


def extend_workflow(workflow: V1alpha1Workflow) -> ExtendedArgoWorkflow:
    return ExtendedArgoWorkflow(workflow, argo_url='https://nonexistentsite.test', access_token='token')


class ArgoArchivedWorkflowsClientTestCase(unittest.TestCase):

    def test__pull_paginated_results_api_function_no_continue_results_one_call(self):
        client = ArgoArchivedWorkflowsClient("https://zombo.com", "tokentokentoken")
        archived_workflows = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        test_mock = MagicMock()
        test_mock.items = archived_workflows
        test_mock.metadata._continue = None

        with patch('argo.workflows.client.ArchivedWorkflowServiceApi.list_archived_workflows',
                   return_value=test_mock) as mock_list_archived_workflows:
            list_results = list(client._pull_paginated_results(mock_list_archived_workflows))
            self.assertEqual(list_results, archived_workflows)
            mock_list_archived_workflows.assert_called_once()

    def test__pull_paginated_results_api_function_one_continue_results_two_calls(self):
        client = ArgoArchivedWorkflowsClient("https://zombo.com", "tokentokentoken")
        archived_workflows = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        test_mock = MagicMock()
        test_mock.items = archived_workflows
        test_mock.metadata._continue = "next_page"

        archived_workflows_2 = [
            mock_argo_workflow('import-hca-total-page2', 'page2uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
        ]

        test_mock2 = MagicMock()
        test_mock2.items = archived_workflows_2
        test_mock2.metadata._continue = None

        def get_page(list_options_continue=None):
            if list_options_continue is None:
                return test_mock

            if list_options_continue == "next_page":
                return test_mock2

        with patch('argo.workflows.client.ArchivedWorkflowServiceApi.list_archived_workflows',
                   side_effect=get_page) as mock_list_archived_workflows:
            list_results = list(client._pull_paginated_results(mock_list_archived_workflows))
            self.assertEqual(list_results, archived_workflows + archived_workflows_2)
            self.assertEqual(mock_list_archived_workflows.call_count, 2)

            mock_list_archived_workflows.assert_has_calls([call(), call(list_options_continue="next_page")])

    def test__pull_paginated_results_api_function_multiple_continues_results_multiple_calls(self):
        client = ArgoArchivedWorkflowsClient("https://zombo.com", "tokentokentoken")
        archived_workflows = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        test_mock = MagicMock()
        test_mock.items = archived_workflows
        test_mock.metadata._continue = "page2"

        archived_workflows_2 = [
            mock_argo_workflow('import-hca-total-page2', 'page2uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
        ]

        test_mock2 = MagicMock()
        test_mock2.items = archived_workflows_2
        test_mock2.metadata._continue = "page3"

        archived_workflows_3 = [
            mock_argo_workflow('import-hca-1', '12345', 'Succeeded', params={
                'data-name': 'dataset',
                'test': 'data'
            }),
            mock_argo_workflow('hca-data', 'u123id', 'Succeeded'),
        ]

        test_mock3 = MagicMock()
        test_mock3.items = archived_workflows_3
        test_mock3.metadata._continue = "page4"

        archived_workflows_4 = [
            mock_argo_workflow('104', 'uid', 'Succeeded', params={
                'dataset': 'data'
            }),
        ]

        test_mock4 = MagicMock()
        test_mock4.items = archived_workflows_4
        test_mock4.metadata._continue = None

        def get_page(list_options_continue=None):
            if list_options_continue is None:
                return test_mock

            if list_options_continue == "page2":
                return test_mock2

            if list_options_continue == "page3":
                return test_mock3

            if list_options_continue == "page4":
                return test_mock4

        with patch('argo.workflows.client.ArchivedWorkflowServiceApi.list_archived_workflows',
                   side_effect=get_page) as mock_list_archived_workflows:
            list_results = list(client._pull_paginated_results(mock_list_archived_workflows))
            self.assertEqual(list_results, archived_workflows + archived_workflows_2 + archived_workflows_3 +
                             archived_workflows_4)
            self.assertEqual(mock_list_archived_workflows.call_count, 4)
            mock_list_archived_workflows.assert_has_calls([call(), call(list_options_continue="page2"),
                                                           call(list_options_continue="page3"),
                                                           call(list_options_continue="page4")])

    def test_inflate_one_workflow_one_call(self):
        v1alphaworkflow_mock = mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Succeeded', params={
            'data-repo-name': 'datarepo_dataset1'
        })

        workflow = extend_workflow(v1alphaworkflow_mock)

        with patch('hca_orchestration.contrib.argo_workflows.ArgoArchivedWorkflowsClient.get_archived_workflow',
                   return_value=v1alphaworkflow_mock) as mock_get_archived_workflow:
            workflow.inflate()
            mock_get_archived_workflow.assert_called_once_with(workflow.metadata.uid)
            workflow.inflate()
            mock_get_archived_workflow.assert_called_once_with(workflow.metadata.uid)

    def test_params_dict_workflow_info_dict_proper_values(self):
        archived_workflows = [
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset1',
                'name': 'value',
                'testing': 'data',
                '1023_3': 'e5Qss'
            }),
            mock_argo_workflow('hca_import', 'randomuid', 'Succeeded', params={
                'dataset_name': 'data'
            }),
            mock_argo_workflow('hca_import', 'randomuid', 'Succeeded'),
        ]

        dict0 = {'data-repo-name': 'datarepo_dataset1',
                 'name': 'value',
                 'testing': 'data',
                 '1023_3': 'e5Qss'}
        dict1 = {
            'dataset_name': 'data'
        }

        workflow0 = extend_workflow(archived_workflows[0])
        workflow1 = extend_workflow(archived_workflows[1])
        workflow2 = extend_workflow(archived_workflows[2])

        self.assertEqual(workflow0.params_dict(), dict0)
        self.assertEqual(workflow1.params_dict(), dict1)
        self.assertEqual(workflow2.params_dict(), {})
