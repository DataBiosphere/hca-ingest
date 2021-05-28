import argparse
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional, Any

from dagster_utils.contrib.data_repo.jobs import poll_job, JobPollException
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import RepositoryApi

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, DefaultHelpParser, get_api_client, query_yes_no, tdr_operation

MAX_DATASET_CREATE_POLL_SECONDS = 120
DATASET_CREATE_POLL_INTERVAL_SECONDS = 2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run(arguments: Optional[list[str]] = None) -> None:
    parser = DefaultHelpParser(description="A simple CLI to manage TDR datasets.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target",
                        choices=["dev", "prod", "real_prod"], required=True)
    subparsers = parser.add_subparsers()

    # create
    dataset_create = subparsers.add_parser("create")
    dataset_create.add_argument("-n", "--dataset_name", help="Name of dataset to create.")
    dataset_create.add_argument("-b", "--billing_profile_id", help="Billing profile ID", required=True)
    dataset_create.add_argument(
        "-p", "--policy-members", help="CSV list of emails to grant steward access to this dataset"
    )
    dataset_create.set_defaults(func=_create_dataset)

    # remove
    dataset_remove = subparsers.add_parser("remove")
    dataset_remove.add_argument("-n", "--dataset_name", help="Name of dataset to delete.")
    dataset_remove.add_argument("-i", "--dataset_id", help="ID of dataset to delete.")
    dataset_remove.set_defaults(func=_remove_dataset)

    # query
    dataset_query = subparsers.add_parser("query")
    dataset_query.add_argument("-n", "--dataset_name", help="Name of dataset to delete.")
    dataset_query.set_defaults(func=_query_dataset)

    args = parser.parse_args(arguments)
    args.func(args)


@tdr_operation
def _remove_dataset(args: argparse.Namespace) -> None:
    if not query_yes_no("Are you sure?"):
        return

    host = data_repo_host[args.env]
    hca = DatasetManager(environment=args.env, data_repo_client=get_api_client(host=host))
    hca.delete_dataset(dataset_name=args.dataset_name, dataset_id=args.dataset_id)


@tdr_operation
def _create_dataset(args: argparse.Namespace) -> None:
    if not query_yes_no("Are you sure?"):
        return

    host = data_repo_host[args.env]

    policy_members = set(args.policy_members.split(','))
    client = get_api_client(host=host)

    hca = DatasetManager(environment=args.env, data_repo_client=client)
    schema = hca.generate_schema()

    hca.create_dataset_with_policy_members(
        args.dataset_name,
        args.billing_profile_id,
        policy_members,
        schema
    )


@tdr_operation
def _query_dataset(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]

    hca = DatasetManager(environment=args.env, data_repo_client=get_api_client(host=host))
    logging.info(hca.enumerate_dataset(dataset_name=args.dataset_name))


@dataclass
class DatasetManager:
    environment: str
    data_repo_client: RepositoryApi

    def generate_schema(self) -> dict[str, object]:
        cwd = os.path.join(os.path.dirname(__file__), "../../../")
        schema_path = f"{cwd}/schema/target/schema.json"
        if not os.path.exists(schema_path):
            subprocess.run(
                ["sbt", f'generateJadeSchema'],
                check=True,
                cwd=cwd
            )

        with open(schema_path) as f:
            parsed: dict[str, object] = json.load(f)
        return parsed

    def create_dataset_with_policy_members(
            self,
            dataset_name: str,
            billing_profile_id: str,
            policy_members: Optional[set[str]],
            schema: dict[str, Any],
            description: Optional[str]
    ) -> str:
        job_id = self.create_dataset(
            dataset_name=dataset_name,
            billing_profile_id=billing_profile_id,
            schema=schema,
            description=description
        )

        try:
            poll_job(
                job_id,
                MAX_DATASET_CREATE_POLL_SECONDS,
                DATASET_CREATE_POLL_INTERVAL_SECONDS,
                self.data_repo_client
            )
        except JobPollException:
            job_result = self.data_repo_client.retrieve_job_result(job_id)
            logging.error("Create job failed, results =")
            logging.error(job_result)
            sys.exit(1)

        job_result = self.data_repo_client.retrieve_job_result(job_id)
        dataset_id: str = job_result["id"]
        logging.info(f"Dataset created, id = {dataset_id}")

        if policy_members:
            logging.info(f"Adding policy_members {policy_members}")
            self.add_policy_members(dataset_id, policy_members, "steward")

        return dataset_id

    def create_dataset(
            self,
            dataset_name: str,
            billing_profile_id: str,
            schema: dict[str, Any],
            description: Optional[str] = None) -> JobId:
        """
        Creates a dataset in the data repo.
        :param dataset_name:  Name of the dataset
        :param billing_profile_id: GCP billing profile ID
        :param schema: Dict containing the dataset's schema
        :param description: Optional description for the dataset
        :return: Job ID of the dataset creation job
        """
        response = self.data_repo_client.create_dataset(
            dataset={
                "name": dataset_name,
                "description": description,
                "defaultProfileId": billing_profile_id,
                "schema": schema,
                "region": "US",
                "cloudPlatform": "gcp"
            }
        )
        job_id: JobId = response.id
        logging.info(f"Dataset creation job id: {job_id}")
        return job_id

    def delete_dataset(self, dataset_name: Optional[str] = None, dataset_id: Optional[str] = None) -> JobId:
        """
        Submits a dataset for deletion. Requires either a dataset ID or name.
        :param dataset_name: Name of the dataset
        :param dataset_id: ID of the dataset
        :return: Job ID of the dataset deletion job
        """
        if dataset_name and not dataset_id:
            response = self.data_repo_client.enumerate_datasets(filter=dataset_name)
            try:
                dataset_id = response.items[0].id
            except IndexError:
                raise ValueError("The provided dataset name returned no results.")
        elif dataset_id and not dataset_name:
            pass  # let dataset_id argument pass through
        else:
            # can't have both/neither provided
            raise ValueError("You must provide either dataset_name or dataset_id, and cannot provide neither/both.")
        delete_response_id: JobId = self.data_repo_client.delete_dataset(dataset_id).id
        logging.info(f"Dataset deletion job id: {delete_response_id}")
        return delete_response_id

    def enumerate_dataset(self, dataset_name: str) -> str:
        """
        Enumerates TDR datasets, filtering on the given dataset_name
        """
        return f"{self.data_repo_client.enumerate_datasets(filter=dataset_name)}"

    def add_policy_members(
            self,
            dataset_id: str,
            policy_members: set[str],
            policy_name: str
    ) -> None:
        """
        Adds the supplied policy members (emails) to the given policy name
        """
        for member in policy_members:
            self.data_repo_client.add_dataset_policy_member(
                dataset_id,
                policy_name=policy_name,
                policy_member={
                    "email": member
                }
            )


if __name__ == '__main__':
    run()
