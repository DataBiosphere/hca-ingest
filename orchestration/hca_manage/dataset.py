import argparse
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from re import search
from typing import Optional, Any

# isort: split

from dagster_utils.contrib.data_repo.jobs import JobPollException, poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import DatasetModel, EnumerateDatasetModel, RepositoryApi

# isort: split

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import (
    DefaultHelpParser,
    data_repo_host,
    get_api_client,
    query_yes_no,
    setup_cli_logging_format,
    tdr_operation,
)

MAX_DATASET_OP_POLL_SECONDS = 300
DATASET_OP_POLL_INTERVAL_SECONDS = 2
DATASET_NAME_REGEX = r"^hca_(dev|prod|staging)_([0-9a-f]{32})?__(\d{4})(\d{2})(\d{2})(?:_([a-zA-Z][a-zA-Z0-9]{0,15}))?$"


class InvalidDatasetNameException(ValueError):
    pass


def run(arguments: Optional[list[str]] = None) -> None:
    setup_cli_logging_format()
    parser = DefaultHelpParser(description="A simple CLI to manage TDR datasets.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target",
                        choices=["dev", "prod", "real_prod"], required=True)
    subparsers = parser.add_subparsers()

    # create
    dataset_create = subparsers.add_parser("create")
    dataset_create.add_argument("-n", "--dataset_name", help="Name of dataset to create.", required=True)
    dataset_create.add_argument("-b", "--billing_profile_id", help="Billing profile ID", required=True)
    dataset_create.add_argument(
        "-p", "--policy-members",
        help="CSV list of emails to grant steward access to this dataset"
    )
    dataset_create.add_argument("-s", "--schema_path", help="Path to JSON schema", required=False)
    dataset_create.add_argument("-r", "--region", help="GCP region for the dataset", required=True)
    dataset_create.add_argument(
        "-v",
        "--validate_dataset_name",
        help="Validate dataset name",
        action=argparse.BooleanOptionalAction,
        default=True)
    dataset_create.set_defaults(func=_create_dataset)

    # remove
    dataset_remove = subparsers.add_parser("remove")
    dataset_remove.add_argument("-n", "--dataset_name", help="Name of dataset to delete.")
    dataset_remove.add_argument("-i", "--dataset_id", help="ID of dataset to delete.")
    dataset_remove.set_defaults(func=_remove_dataset)

    # query
    dataset_query = subparsers.add_parser("query")
    dataset_query.add_argument("-n", "--dataset_name", help="Name of dataset to filter for")
    dataset_query.set_defaults(func=_query_dataset)

    # retrieve
    dataset_retrieve = subparsers.add_parser("retrieve")
    dataset_retrieve.add_argument("-i", "--id", help="UUID of dataset to retrieve")
    dataset_retrieve.set_defaults(func=_retrieve_dataset)

    args = parser.parse_args(arguments)
    args.func(args)


@tdr_operation
def _remove_dataset(args: argparse.Namespace) -> None:
    if args.dataset_id:
        prompt = f"This will remove dataset id = {args.dataset_id} in env = {args.env}. Are you sure?"
    else:
        prompt = f"This will remove dataset name = {args.dataset_name} in env = {args.env}. Are you sure?"

    if not query_yes_no(prompt):
        return

    host = data_repo_host[args.env]
    hca = DatasetManager(environment=args.env, data_repo_client=get_api_client(host=host))

    hca.delete_dataset(dataset_name=args.dataset_name, dataset_id=args.dataset_id)


# validate provided name against dataset name regex from the spec
def _validate_dataset_name(dataset_name: str) -> None:
    if not search(DATASET_NAME_REGEX, dataset_name):
        raise InvalidDatasetNameException(f"Dataset name {dataset_name} is invalid")


@tdr_operation
def _create_dataset(args: argparse.Namespace) -> None:
    if args.validate_dataset_name:
        _validate_dataset_name(args.dataset_name)
    if not query_yes_no(f"This will create dataset name = {args.dataset_name} in env = {args.env}. Are you sure?"):
        return

    host = data_repo_host[args.env]
    region = args.region

    policy_members = None
    if args.policy_members:
        policy_members = set(args.policy_members.split(','))

    client = get_api_client(host=host)

    hca = DatasetManager(environment=args.env, data_repo_client=client)

    if args.schema_path:
        with open(args.schema_path, "r") as f:
            schema = json.load(f)
    else:
        schema = hca.generate_schema()

    dataset_model = hca.create_dataset_with_policy_members(
        args.dataset_name,
        args.billing_profile_id,
        policy_members,
        schema,
        region,
        args.env,
        None
    )

    logging.info({
        'id': dataset_model.id,
        'default_profile_id': dataset_model.default_profile_id,
        'data_project': dataset_model.data_project,
        'name': dataset_model.name
    })


@tdr_operation
def _query_dataset(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]

    hca = DatasetManager(environment=args.env, data_repo_client=get_api_client(host=host))
    logging.info(hca.enumerate_dataset(dataset_name=args.dataset_name))


@tdr_operation
def _retrieve_dataset(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]

    hca = DatasetManager(environment=args.env, data_repo_client=get_api_client(host=host))
    logging.info(hca.retrieve_dataset(dataset_id=args.id))


@dataclass
class DatasetManager:
    environment: str
    data_repo_client: RepositoryApi
    stewards: set[str] = field(init=False)

    def __post_init__(self) -> None:
        self.stewards = {
            "dev": {
                "monster-dev@dev.test.firecloud.org",
                "hca-dagster-runner@broad-dsp-monster-hca-dev.iam.gserviceaccount.com"
            },
            "prod": {"monster@firecloud.org", "hca-dagster-runner@mystical-slate-284720.iam.gserviceaccount.com"},
            "real_prod": {"monster@firecloud.org", "hca-dagster-runner@mystical-slate-284720.iam.gserviceaccount.com"},
        }[self.environment]

    def generate_schema(self) -> dict[str, object]:
        cwd = os.path.join(os.path.dirname(__file__), "../")
        schema_path = f"{cwd}/schema.json"  # noqa: F541
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
            region: str,
            env: str,
            description: Optional[str]
    ) -> DatasetModel:
        job_id = self.create_dataset(
            dataset_name=dataset_name,
            billing_profile_id=billing_profile_id,
            schema=schema,
            description=description,
            region=region,
            env=env
        )

        self._poll_job(job_id)

        job_result = self.data_repo_client.retrieve_job_result(job_id)
        dataset_id: str = job_result["id"]
        logging.info(f"Dataset created, id = {dataset_id}")

        logging.info(f"Adding default policy_members {self.stewards}")
        self.add_policy_members(dataset_id, self.stewards, "steward")
        if policy_members:
            logging.info(f"Adding policy_members {policy_members}")
            self.add_policy_members(dataset_id, policy_members, "steward")

        return self.retrieve_dataset(dataset_id)

    def create_dataset(
            self,
            dataset_name: str,
            billing_profile_id: str,
            schema: dict[str, Any],
            region: str,
            env: str,
            description: Optional[str] = None) -> JobId:
        """
        Creates a dataset in the data repo.
        Hardcodes the dedicatedIngestServiceAccount to false, as this is set to true by default in the data repo,
        and we don't want dedicated SAs for this interim solution. (see FE-39)
        :param dataset_name:  Name of the dataset
        :param billing_profile_id: GCP billing profile ID
        :param schema: Dict containing the dataset's schema
        :param description: Optional description for the dataset
        :param region GCP region in which to create this dataset
        :param: Jade env (prod, dev, real_prod, etc.)
        :return: Job ID of the dataset creation job
        """
        payload = {
            "name": dataset_name,
            "description": description,
            "defaultProfileId": billing_profile_id,
            "schema": schema,
            "region": region,
            "cloudPlatform": "gcp",
            "dedicatedIngestServiceAccount": "false",
        }

        response = self.data_repo_client.create_dataset(
            dataset=payload
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

        # set a request timeout (read + connect) of 30 seconds; for unknown reasons, this call can hang for
        # minutes in our CI environment. Setting a timeout here forces a quick failure + an immediate
        # (hoppefully successful) retry
        delete_response_id: JobId = self.data_repo_client.delete_dataset(dataset_id, _request_timeout=30).id
        logging.info(f"Dataset deletion job id: {delete_response_id}")

        self._poll_job(delete_response_id)
        return delete_response_id

    def _poll_job(self, job_id: JobId) -> None:
        try:
            poll_job(
                job_id,
                MAX_DATASET_OP_POLL_SECONDS,
                DATASET_OP_POLL_INTERVAL_SECONDS,
                self.data_repo_client
            )
        except JobPollException:
            job_result = self.data_repo_client.retrieve_job_result(job_id)
            logging.error("Dataset job failed, results =")
            logging.error(job_result)
            sys.exit(1)

    def enumerate_dataset(self, dataset_name: str) -> EnumerateDatasetModel:
        """
        Enumerates TDR datasets, filtering on the given dataset_name
        """
        return self.data_repo_client.enumerate_datasets(filter=dataset_name, limit=1000)

    def retrieve_dataset(self, dataset_id: str) -> DatasetModel:
        return self.data_repo_client.retrieve_dataset(dataset_id)

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
