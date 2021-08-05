from dagster import InitResourceContext, resource, String

from hca_orchestration.models.hca_dataset import HcaDataset


@resource({
    "dataset_name": String,
    "dataset_id": String,
    "project_id": String,
    "billing_profile_id": String,
})
def target_hca_dataset(init_context: InitResourceContext) -> HcaDataset:
    return HcaDataset(**init_context.resource_config)
