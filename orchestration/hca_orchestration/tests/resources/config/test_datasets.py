from unittest.mock import Mock

from dagster import build_init_resource_context, ResourceDefinition

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.resources.config.datasets import find_or_create_project_dataset
from hca_orchestration.models.hca_dataset import TdrDataset


def test_find_or_create_project_dataset_returns_existing_dataset():
    data_repo_service = Mock(spec=DataRepoService)
    data_repo_service.find_dataset = Mock(
        return_value=TdrDataset(
            dataset_name="fake_name",
            dataset_id="fake_dataset_id",
            project_id="fake_gcp_proj_id",
            billing_profile_id="fake_billing_prof_id",
            bq_location="fake_bq_location"
        )
    )
    init_context = build_init_resource_context(
        config={
            "env": "dev",
            "region": "US",
            "policy_members": ["example@example.com"],
            "billing_profile_id": "fake_billing_profile_id",
            "qualifier": None
        },
        resources={
            "hca_project_id": ResourceDefinition.hardcoded_resource("08BCA7FF-A15A-4D58-806B-7CD45979768B"),
            "data_repo_service": ResourceDefinition.hardcoded_resource(data_repo_service),
            "run_start_time": ResourceDefinition.hardcoded_resource(1639494828)
        }
    )

    result = find_or_create_project_dataset(init_context)

    data_repo_service.find_dataset.assert_called_once_with(
        "hca_dev_08bca7ffa15a4d58806b7cd45979768b", None
    )
    assert result.dataset_id == "fake_dataset_id", "Should receive the matching dataset"


def test_find_or_create_project_dataset_creates_new_dataset():
    data_repo_service = Mock(spec=DataRepoService)
    data_repo_service.find_dataset = Mock(return_value=None)
    init_context = build_init_resource_context(
        config={
            "env": "dev",
            "region": "US",
            "policy_members": ["example@example.com"],
            "billing_profile_id": "fake_billing_profile_id",
            "qualifier": None
        },
        resources={
            "hca_project_id": ResourceDefinition.hardcoded_resource("08BCA7FF-A15A-4D58-806B-7CD45979768B"),
            "data_repo_service": ResourceDefinition.hardcoded_resource(data_repo_service),
            "run_start_time": ResourceDefinition.hardcoded_resource(1639494828)
        }
    )

    find_or_create_project_dataset(init_context)

    data_repo_service.find_dataset.assert_called_once_with(
        "hca_dev_08bca7ffa15a4d58806b7cd45979768b", None
    )
    data_repo_service.create_dataset.assert_called_once_with(
        'hca_dev_08bca7ffa15a4d58806b7cd45979768b__20211214',
        'dev',
        'fake_billing_profile_id',
        ['example@example.com'],
        'US',
        '{"hca_project_id": "08bca7ffa15a4d58806b7cd45979768b"}'
    )


def test_find_or_create_project_dataset_creates_new_dataset_with_qualifier():
    data_repo_service = Mock(spec=DataRepoService)
    data_repo_service.find_dataset = Mock(return_value=None)
    init_context = build_init_resource_context(
        config={
            "env": "dev",
            "region": "US",
            "policy_members": ["example@example.com"],
            "billing_profile_id": "fake_billing_profile_id",
            "qualifier": "test_qualifier"
        },
        resources={
            "hca_project_id": ResourceDefinition.hardcoded_resource("08BCA7FF-A15A-4D58-806B-7CD45979768B"),
            "data_repo_service": ResourceDefinition.hardcoded_resource(data_repo_service),
            "run_start_time": ResourceDefinition.hardcoded_resource(1639494828)
        }
    )

    find_or_create_project_dataset(init_context)

    data_repo_service.find_dataset.assert_called_once_with(
        "hca_dev_08bca7ffa15a4d58806b7cd45979768b", "test_qualifier"
    )
    data_repo_service.create_dataset.assert_called_once_with(
        'hca_dev_08bca7ffa15a4d58806b7cd45979768b__20211214_test_qualifier',
        'dev',
        'fake_billing_profile_id',
        ['example@example.com'],
        'US',
        '{"hca_project_id": "08bca7ffa15a4d58806b7cd45979768b"}'
    )


def test_find_or_create_project_dataset_transforms_real_prod_to_prod():
    data_repo_service = Mock(spec=DataRepoService)
    init_context = build_init_resource_context(
        config={
            "env": "real_prod",
            "region": "US",
            "policy_members": ["example@example.com"],
            "billing_profile_id": "fake_billing_profile_id",
            "qualifier": None
        },
        resources={
            "hca_project_id": ResourceDefinition.hardcoded_resource("08BCA7FF-A15A-4D58-806B-7CD45979768B"),
            "data_repo_service": ResourceDefinition.hardcoded_resource(data_repo_service),
            "run_start_time": ResourceDefinition.hardcoded_resource(1639494828)
        }
    )

    find_or_create_project_dataset(init_context)

    data_repo_service.find_dataset.assert_called_once_with(
        "hca_prod_08bca7ffa15a4d58806b7cd45979768b", None
    )
