from argo.workflows.client import ApiClient as ArgoApiClient,\
                                  ArchivedWorkflowServiceApi,\
                                  Configuration as ArgoConfiguration
from argo.workflows.client.models import V1alpha1Workflow


def generate_argo_archived_workflows_client(host_url, access_token):
    return ArchivedWorkflowServiceApi(
        api_client=ArgoApiClient(
            configuration=ArgoConfiguration(host=host_url),
            header_name="Authorization",
            header_value=f"Bearer {access_token}"))


class ArgoArchivedWorkflowsClientMixin:
    def __init__(self, *args, argo_url, access_token, **kwargs):
        super().__init__(*args, **kwargs)
        self.argo_url = argo_url
        self.access_token = access_token
        self._client = None

    def client(self):
        if not self._client:
            self._client = generate_argo_archived_workflows_client(self.argo_url, self.access_token)

        return self._client

    def list_archived_workflows(self):
        self._pull_paginated_results(self.client().list_archived_workflows)

    def _pull_paginated_results(self, api_function):
        results = api_function()

        for result in results.items:
            yield result

        while results.metadata['_continue']:
            results = api_function(list_option_continue=results.metadata['_continue'])

            for result in results.items:
                yield result


class ExtendedArgoWorkflow(ArgoArchivedWorkflowsClientMixin):
    def __init__(self, workflow: V1alpha1Workflow, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._workflow = workflow
        self._inflated = False

    def inflate(self):
        if not self._inflated:
            self._workflow = self.client().get_archived_workflow(self.metadata.uid)
            self._inflated = True

        return self

    def params_dict(self):
        return {
            param.name: param.value
            for param in self.spec.arguments.parameters
        }

    def __getattr__(self, name):
        return getattr(self._workflow, name)

    def __eq__(self, other):
        return isinstance(other, ExtendedArgoWorkflow) and self._workflow == other._workflow
