from dagster import InitResourceContext, resource, String

from hca_orchestration.models.hca_dataset import TdrDataset


@resource({
    "dataset_name": String,
    "dataset_id": String,
    "project_id": String,
    "billing_profile_id": String,
})
def target_hca_dataset(init_context: InitResourceContext) -> TdrDataset:
    return TdrDataset(**init_context.resource_config)
