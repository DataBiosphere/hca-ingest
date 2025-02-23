from dataclasses import dataclass

from dagster import resource, StringSource, Field, InitResourceContext
from data_repo_client import (
    ApiClient,
    RepositoryApi,
    PolicyMemberRequest,
    PolicyResponse,
    PolicyModel,
    SnapshotModel,
)

from hca.contrib.google import get_credentials
from hca.contrib.terra.data_repo.api_configuration import (
    RefreshingAccessTokenConfig,
)


def build_client(host: str) -> RepositoryApi:
    creds = get_credentials()
    config = RefreshingAccessTokenConfig(host=host, google_credentials=creds)
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    return RepositoryApi(api_client=client)


@resource({"api_url": Field(StringSource)})
def terra_data_repo_client(init_context: InitResourceContext) -> RepositoryApi:
    return build_client(init_context.resource_config["api_url"])


class LocalDataRepoClient:
    @dataclass
    class NoopResult:
        total: int

    @dataclass
    class FakeJobResponse:
        completed: bool
        id: str
        job_status: str

    def create_snapshot(self, snapshot: dict[str, str]) -> FakeJobResponse:
        return LocalDataRepoClient.FakeJobResponse(True, "abcdef", "succeeded")

    def enumerate_datasets(self) -> NoopResult:
        return LocalDataRepoClient.NoopResult(5)

    def retrieve_job(self, job_id: str) -> FakeJobResponse:
        return LocalDataRepoClient.FakeJobResponse(True, "abcdef", "succeeded")

    def bulk_file_load(
            self, dataset_id: str, bulk_file_load: dict[str, str]
    ) -> FakeJobResponse:
        return LocalDataRepoClient.FakeJobResponse(True, "abcdef", "succeeded")

    def retrieve_job_result(self, job_id: str) -> dict[str, object]:
        return {"id": "fake_object_id", "name": "fake_object_name", "failedFiles": 0}

    def apply_dataset_data_deletion(
            self, id: str, data_deletion_request: dict[str, object]
    ) -> FakeJobResponse:
        return LocalDataRepoClient.FakeJobResponse(True, "abcdef", "succeeded")

    def ingest_dataset(self, id: str, ingest: dict[str, object]) -> FakeJobResponse:
        return LocalDataRepoClient.FakeJobResponse(True, "abcdef", "succeeded")

    def add_snapshot_policy_member(
            self, id: str, policy_name: str, policy_member: PolicyMemberRequest
    ) -> PolicyResponse:
        return PolicyResponse(
            policies=[PolicyModel(name=policy_name, members=[policy_member])]
        )

    def retrieve_snapshot(self, id: str, include: list[str]) -> SnapshotModel:
        return SnapshotModel(id=id, name="fake")


@resource
def local_terra_data_repo_client(
        init_context: InitResourceContext,
) -> LocalDataRepoClient:
    return LocalDataRepoClient()
