from typing import Iterator

from dagster import AssetMaterialization, EventMetadataEntry, Output, OutputDefinition, solid, Failure, Optional, \
    Noneable, Field, Permissive, InitResourceContext
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from data_repo_client import RepositoryApi, PolicyMemberRequest, PolicyResponse

from hca_manage.common import JobId
from hca_manage.snapshot import SnapshotManager, query_snapshot
from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService


@solid(
    config_schema=Field(Permissive({"validate_snapshot_name": Field(bool, default_value=True, is_required=False)})),
    required_resource_keys={'data_repo_client', 'snapshot_config', 'hca_manage_config', 'data_repo_service'},
)
def submit_snapshot_job(context: AbstractComputeExecutionContext) -> JobId:
    data_repo_service: DataRepoService = context.resources.data_repo_service
    dataset_name = context.resources.snapshot_config.dataset_name
    dataset = data_repo_service.find_dataset(dataset_name)
    if not dataset:
        raise Failure(f"Dataset not found for dataset name [dataset_name={dataset_name}]")

    context.log.info(f"Source dataset for snapshot = {dataset_name}")
    return SnapshotManager(
        environment=context.resources.hca_manage_config.gcp_env,
        dataset=context.resources.snapshot_config.dataset_name,
        data_repo_client=context.resources.data_repo_client,
        data_repo_profile_id=dataset.billing_profile_id
    ).submit_snapshot_request_with_name(
        context.resources.snapshot_config.snapshot_name,
        context.resources.snapshot_config.managed_access,
        context.solid_config["validate_snapshot_name"],
    )


@solid(
    required_resource_keys={'data_repo_client', 'snapshot_config'},
    output_defs=[
        OutputDefinition(name='result', dagster_type=str)
    ],
)
def get_completed_snapshot_info(context: AbstractComputeExecutionContext, job_id: JobId) -> Iterator[Output]:
    # retrieve_job_result returns a raw dict (since it can return many kinds of data), so we need to make
    # a second call to the snapshot endpoint to get the actual SnapshotModel from it
    snapshot_info_dict = context.resources.data_repo_client.retrieve_job_result(job_id)
    snapshot_details = context.resources.data_repo_client.retrieve_snapshot(
        id=snapshot_info_dict['id'], include=["PROFILE,DATA_PROJECT"])

    yield AssetMaterialization(
        asset_key=snapshot_info_dict['id'],
        description="Dataset snapshot created in the data repo",
        metadata_entries=[
            EventMetadataEntry.text(
                context.resources.snapshot_config.dataset_name,
                "dataset_name",
                description="Dataset name in the data repo"),
            EventMetadataEntry.text(
                snapshot_info_dict['name'],
                "snapshot_name",
                description="Snapshot name in the data repo"),
            EventMetadataEntry.text(snapshot_info_dict['id'], "snapshot_id",
                                    description="Snapshot ID in the data repo"),
            EventMetadataEntry.text(job_id, "job_id", description="Successful data repo job ID"),
        ],
        tags={
            "snapshot_id": snapshot_info_dict['id'],
            "data_project": snapshot_details.data_project,
            "snapshot_name": snapshot_info_dict['name']
        }
    )
    yield Output(snapshot_info_dict['id'])

@solid(
    config_schema=Field(Permissive({"validate_snapshot_name": Field(bool, default_value=True, is_required=False)})),
    required_resource_keys={'data_repo_client', 'snapshot_config'},
)
# use project_id to get snapshot_id from TDR see datasets.py? for how to do that
# enumerateSnapshots filter on project_id
# looks like actually we get the dataset_name from the job context so... we can just search for that as it is the same as the snapshot_name
 ## "hca_{env}_{project_id}__{datetime.today().date()}_dcp2_{release_tag}"
# if returns 0 fail - look for examples
# if returns > 1 fail ->> might need to start tagging snapshots with release_tag
# get the snapshot_name too - and verify that ends in release_tag
def get_snapshot_from_project(context: AbstractComputeExecutionContext, init_context: InitResourceContext) -> str:
    dataset_name = context.resources.snapshot_config.dataset_name
    snapshot = query_snapshot(filter=dataset_name, limit=1)
    name = snapshot.name
    # are context & init_context the same??
    release_tag = init_context.resource_config['qualifier']

    if not snapshot:
        raise Failure(f"Snapshot not found for dataset name [dataset_name={dataset_name}]")
    if snapshot:
        if not name.endswith(release_tag):
            raise Failure(f"Snapshot name does not end in release tag [snapshot_name={name}]")
        else:
            return snapshot.id

@solid(
    required_resource_keys={'sam_client'},
)
def make_snapshot_public(context: AbstractComputeExecutionContext, snapshot_id: str) -> str:
    context.resources.sam_client.set_public_flag(snapshot_id, True)
    return snapshot_id


@solid(
    config_schema={"snapshot_steward": str},
    required_resource_keys={'data_repo_client'}
)
def add_steward(context: AbstractComputeExecutionContext, snapshot_id: str) -> str:
    data_repo_client: RepositoryApi = context.resources.data_repo_client
    policy_member = context.solid_config["snapshot_steward"]

    result: PolicyResponse = data_repo_client.add_snapshot_policy_member(
        id=snapshot_id, policy_name="steward", policy_member=PolicyMemberRequest(email=policy_member))
    for policy in result.policies:
        if policy.name == 'steward':
            found_member = False
            for member in policy.members:
                if policy_member == member:
                    found_member = True

            if not found_member:
                raise Failure(f"Policy member {policy_member} not added to stewards for snapshot id {snapshot_id}")

    return snapshot_id
