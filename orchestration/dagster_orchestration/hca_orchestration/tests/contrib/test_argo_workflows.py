import unittest

from unittest.mock import MagicMock, patch, call
from typing import Generator, TypeVar, Optional, Iterator
from collections.abc import Iterable

from hca_orchestration.contrib.argo_workflows import ExtendedArgoWorkflow, generate_argo_archived_workflows_client, \
    ArgoFetchListOperation, ArgoArchivedWorkflowsClient
from argo.workflows.client.models import V1alpha1Workflow, V1ObjectMeta, V1alpha1WorkflowSpec, V1alpha1Arguments, \
    V1alpha1Parameter, \
    V1alpha1WorkflowStatus

from hca_orchestration.tests.support.mock_workflows import mock_argo_workflow, extend_workflow


class ArgoArchivedWorkflowsClientTestCase(unittest.TestCase):
    def setUp(self):
        self.client = ArgoArchivedWorkflowsClient("https://zombo.com", "tokentokentoken")

    def workflows_to_page_of_results(
            self, archived_workflows: list[V1alpha1Workflow], next_page: Optional[str]) -> MagicMock:
        results_page = MagicMock()
        results_page.items = archived_workflows
        results_page.metadata._continue = next_page
        return results_page

    def test__pull_paginated_results_api_function_no_continue_results_one_call(self):
        workflows_on_page_one = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        archived_workflow_list = self.workflows_to_page_of_results(workflows_on_page_one, None)

        with patch('argo.workflows.client.ArchivedWorkflowServiceApi.list_archived_workflows',
                   return_value=archived_workflow_list) as mock_list_archived_workflows:
            list_results = list(self.client._pull_paginated_results(mock_list_archived_workflows))
            self.assertEqual(list_results, workflows_on_page_one)
            mock_list_archived_workflows.assert_called_once()

    def test__pull_paginated_results_api_function_one_continue_results_two_calls(self):
        workflows_on_page_one = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        page_one_response = self.workflows_to_page_of_results(workflows_on_page_one,
                                                              "next_page")

        workflows_on_page_two = [
            mock_argo_workflow('import-hca-total-page2', 'page2uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
        ]

        page_two_response = self.workflows_to_page_of_results(workflows_on_page_two,
                                                              None)

        def get_page(list_options_continue=None):
            if list_options_continue is None:
                return page_one_response

            if list_options_continue == "next_page":
                return page_two_response

        with patch('argo.workflows.client.ArchivedWorkflowServiceApi.list_archived_workflows',
                   side_effect=get_page) as mock_list_archived_workflows:
            list_results = list(self.client._pull_paginated_results(mock_list_archived_workflows))
            self.assertEqual(list_results, workflows_on_page_one + workflows_on_page_two)
            self.assertEqual(mock_list_archived_workflows.call_count, 2)

            mock_list_archived_workflows.assert_has_calls([call(), call(list_options_continue="next_page")])

    def test__pull_paginated_results_api_function_multiple_continues_results_multiple_calls(self):
        workflows_on_page_one = [
            mock_argo_workflow('import-hca-total-defg', 'abc123uid', 'Succeeded'),
            mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
            mock_argo_workflow('import-hca-total-cdef', 'abc345uid', 'Succeeded', params={
                'data-repo-name': 'datarepo_dataset2'
            }),
        ]

        page_one_response = self.workflows_to_page_of_results(workflows_on_page_one,
                                                              "page2")

        workflows_on_page_two = [
            mock_argo_workflow('import-hca-total-page2', 'page2uid', 'Supsneeded', params={
                'data-repo-name': 'datarepo_dataset1'
            }),
        ]

        page_two_response = self.workflows_to_page_of_results(workflows_on_page_two,
                                                              "page3")

        workflows_on_page_three = [
            mock_argo_workflow('import-hca-1', '12345', 'Succeeded', params={
                'data-name': 'dataset',
                'test': 'data'
            }),
            mock_argo_workflow('hca-data', 'u123id', 'Succeeded'),
        ]

        page_three_response = self.workflows_to_page_of_results(workflows_on_page_three,
                                                                "page4")

        workflows_on_page_four = [
            mock_argo_workflow('hca-import', 'uid', 'Succeeded', params={
                'testing_data': 'dataset'
            }),
        ]

        page_four_response = self.workflows_to_page_of_results(workflows_on_page_four,
                                                               None)

        def get_page(list_options_continue=None):
            page_response = {
                None: page_one_response,
                "page2": page_two_response,
                "page3": page_three_response,
                "page4": page_four_response
            }[list_options_continue]
            return page_response

        with patch('argo.workflows.client.ArchivedWorkflowServiceApi.list_archived_workflows',
                   side_effect=get_page) as mock_list_archived_workflows:
            list_results = list(self.client._pull_paginated_results(mock_list_archived_workflows))
            self.assertEqual(list_results,
                             workflows_on_page_one +
                             workflows_on_page_two +
                             workflows_on_page_three +
                             workflows_on_page_four)
            self.assertEqual(mock_list_archived_workflows.call_count, 4)
            mock_list_archived_workflows.assert_has_calls([call(),
                                                           call(list_options_continue="page2"),
                                                           call(list_options_continue="page3"),
                                                           call(list_options_continue="page4")])

    def test_inflate_one_workflow_one_call(self):
        mock_inflated_workflow = mock_argo_workflow('import-hca-total-abcd', 'abc234uid', 'Succeeded', params={
            'data-repo-name': 'datarepo_dataset1'
        })

        workflow = extend_workflow(mock_inflated_workflow)

        with patch('hca_orchestration.contrib.argo_workflows.ArgoArchivedWorkflowsClient.get_archived_workflow',
                   return_value=mock_inflated_workflow) as mock_get_archived_workflow:
            workflow.inflate()
            mock_get_archived_workflow.assert_called_once_with(workflow.metadata.uid)
            # Checking that despite inflate called twice, mock_get_archived_workflow is only called once
            workflow.inflate()
            mock_get_archived_workflow.assert_called_once_with(workflow.metadata.uid)

    def test_params_dict_workflow_info_dict_proper_values(self):
        params_for_argo_workflow_large = {'data-repo-name': 'datarepo_dataset1',
                                          'name': 'value',
                                          'testing': 'data',
                                          '1023_3': 'e5Qss'}
        params_for_argo_workflow_small = {
            'dataset_name': 'data'
        }
        archived_workflows = [
            mock_argo_workflow(
                'import-hca-total-abcd',
                'abc234uid',
                'Succeeded',
                params=params_for_argo_workflow_large),
            mock_argo_workflow('hca_import', 'randomuid', 'Succeeded', params=params_for_argo_workflow_small),
            mock_argo_workflow('hca_import', 'randomuid', 'Succeeded'),
        ]

        list_workflows = [extend_workflow(workflow) for workflow in archived_workflows]

        self.assertEqual(list_workflows[0].params_dict(), params_for_argo_workflow_large)
        self.assertEqual(list_workflows[1].params_dict(), params_for_argo_workflow_small)
        self.assertEqual(list_workflows[2].params_dict(), {})
