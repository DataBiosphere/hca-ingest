import unittest

from unittest.mock import MagicMock, patch, call
from typing import Generator, TypeVar, Optional
from collections.abc import Iterable

from hca_orchestration.contrib.argo_workflows import ExtendedArgoWorkflow, generate_argo_archived_workflows_client, \
    ArgoFetchListOperation, ArgoArchivedWorkflowsClient
from argo.workflows.client.models import V1alpha1Workflow, V1ObjectMeta, V1alpha1WorkflowSpec, V1alpha1Arguments, \
    V1alpha1Parameter, \
    V1alpha1WorkflowStatus

from hca_orchestration.tests.support.mock_workflows import mock_argo_workflow


def extend_workflow(workflow: V1alpha1Workflow) -> ExtendedArgoWorkflow:
    return ExtendedArgoWorkflow(workflow, argo_url='https://nonexistentsite.test', access_token='token')


class ArgoArchivedWorkflowsClientTestCase(unittest.TestCase):
    def setUp(self):
        self.client = ArgoArchivedWorkflowsClient("https://zombo.com", "tokentokentoken")

    def workflows_to_page_of_results(self, archived_workflows: [], next_page: Optional[str]):
        results_page = MagicMock()
        results_page.items = archived_workflows
        results_page.metadata._continue = next_page
        return results_page

    def test__pull_paginated_results_api_function_no_continue_results_one_call(self):
        list_containing_3_V1alpha1Workflows = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        archived_workflow_list = self.workflows_to_page_of_results(list_containing_3_V1alpha1Workflows, None)

        with patch('argo.workflows.client.ArchivedWorkflowServiceApi.list_archived_workflows',
                   return_value=archived_workflow_list) as mock_list_archived_workflows:
            list_results = list(self.client._pull_paginated_results(mock_list_archived_workflows))
            self.assertEqual(list_results, list_containing_3_V1alpha1Workflows)
            mock_list_archived_workflows.assert_called_once()

    def test__pull_paginated_results_api_function_one_continue_results_two_calls(self):
        list_containing_3_V1alpha1Workflows = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        archived_workflow_list_3_workflows = self.workflows_to_page_of_results(list_containing_3_V1alpha1Workflows,
                                                                               "next_page")

        list_containing_1_V1alpha1Workflows = [
            mock_argo_workflow('import-hca-total-page2', 'page2uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
        ]

        archived_workflow_list_1_workflows = self.workflows_to_page_of_results(list_containing_1_V1alpha1Workflows,
                                                                               None)

        def get_page(list_options_continue=None):
            if list_options_continue is None:
                return archived_workflow_list_3_workflows

            if list_options_continue == "next_page":
                return archived_workflow_list_1_workflows

        with patch('argo.workflows.client.ArchivedWorkflowServiceApi.list_archived_workflows',
                   side_effect=get_page) as mock_list_archived_workflows:
            list_results = list(self.client._pull_paginated_results(mock_list_archived_workflows))
            self.assertEqual(list_results, list_containing_3_V1alpha1Workflows + list_containing_1_V1alpha1Workflows)
            self.assertEqual(mock_list_archived_workflows.call_count, 2)

            mock_list_archived_workflows.assert_has_calls([call(), call(list_options_continue="next_page")])

    def test__pull_paginated_results_api_function_multiple_continues_results_multiple_calls(self):
        list_containing_3_V1alpha1Workflows = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        archived_workflow_list_3_workflows = self.workflows_to_page_of_results(list_containing_3_V1alpha1Workflows,
                                                                               "page2")

        list_containing_1_V1alpha1Workflows = [
            mock_argo_workflow('import-hca-total-page2', 'page2uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
        ]

        archived_workflow_list_1_workflows = self.workflows_to_page_of_results(list_containing_1_V1alpha1Workflows,
                                                                               "page3")

        list_containing_2_V1alpha1Workflows = [
            mock_argo_workflow('import-hca-1', '12345', 'Succeeded', params={
                'data-name': 'dataset',
                'test': 'data'
            }),
            mock_argo_workflow('hca-data', 'u123id', 'Succeeded'),
        ]

        archived_workflow_list_2_workflows = self.workflows_to_page_of_results(list_containing_2_V1alpha1Workflows,
                                                                               "page4")

        list_containing_0_V1alpha1Workflows = [
        ]

        archived_workflow_list_0_workflows = self.workflows_to_page_of_results(list_containing_0_V1alpha1Workflows,
                                                                               None)

        def get_page(list_options_continue=None):
            {
                None: archived_workflow_list_3_workflows,
                "page2": archived_workflow_list_1_workflows,
                "page3": archived_workflow_list_2_workflows,
                "page4": archived_workflow_list_0_workflows
            }[list_options_continue]

            list_results = list(self.client._pull_paginated_results(get_page()))
            self.assertEqual(list_results,
                             list_containing_3_V1alpha1Workflows +
                             list_containing_1_V1alpha1Workflows +
                             list_containing_2_V1alpha1Workflows +
                             list_containing_0_V1alpha1Workflows)
            self.assertEqual(list_results.call_count, 4)
            list_results.assert_has_calls([call(),
                                           call(list_options_continue="page2"),
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
            # Checking that despite inflate called twice, mock_get_archived_workflow is only called once
            workflow.inflate()
            mock_get_archived_workflow.assert_called_once_with(workflow.metadata.uid)

    def test_params_dict_workflow_info_dict_proper_values(self):
        dict0 = {'data-repo-name': 'datarepo_dataset1',
                 'name': 'value',
                 'testing': 'data',
                 '1023_3': 'e5Qss'}
        dict1 = {
            'dataset_name': 'data'
        }
        archived_workflows = [
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Succeeded', params=dict0),
            mock_argo_workflow('hca_import', 'randomuid', 'Succeeded', params=dict1),
            mock_argo_workflow('hca_import', 'randomuid', 'Succeeded'),
        ]

        list_workflows = [extend_workflow(workflow) for workflow in archived_workflows]

        self.assertEqual(list_workflows[0].params_dict(), dict0)
        self.assertEqual(list_workflows[1].params_dict(), dict1)
        self.assertEqual(list_workflows[2].params_dict(), {})
