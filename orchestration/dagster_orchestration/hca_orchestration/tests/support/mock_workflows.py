from datetime import datetime
from dateutil.tz import tzlocal
from argo.workflows.client.models import V1alpha1Workflow, V1ObjectMeta, V1alpha1WorkflowSpec, V1alpha1Arguments, V1alpha1Parameter, \
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
