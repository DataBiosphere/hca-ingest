import argparse
from dataclasses import dataclass
import json
import logging
from typing import Optional

from data_repo_client import RepositoryApi

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, DefaultHelpParser, JobId, get_api_client, query_yes_no


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run(arguments: Optional[list[str]] = None) -> None:
    parser = DefaultHelpParser(description="A simple CLI to manage TDR datasets.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target", choices=["dev", "prod"], required=True)

    dataset_flags = parser.add_mutually_exclusive_group(required=True)
    dataset_flags.add_argument("-c", "--create", help="Flag to create a dataset", action="store_true")
    dataset_flags.add_argument("-r", "--remove", help="Flag to delete a dataset", action="store_true")
    dataset_flags.add_argument("-q", "--query", help="Flag to query a dataset ID", action="store_true")

    # create
    dataset_create_args = parser.add_argument_group()
    dataset_create_args.add_argument("-b", "--billing_profile_id", help="Billing profile ID")
    dataset_create_args.add_argument("-j", "--schema_path", help="Path to table schema (JSON)", required=True)

    # delete
    dataset_delete_args = parser.add_mutually_exclusive_group(required=True)
    dataset_delete_args.add_argument("-n", "--dataset_name", help="Name of dataset to delete.")
    dataset_delete_args.add_argument("-i", "--dataset_id", help="ID of dataset to delete.")

    args = parser.parse_args(arguments)
    host = data_repo_host[args.env]

    if args.remove:
        if query_yes_no("Are you sure?"):
            remove_dataset(args, host)
        else:
            # TODO change to logger?
            print("No deletion attempted.")
    elif args.create:
        if query_yes_no("This will create a dataset. Are you sure?"):
            create_dataset(args, host)
    elif args.query:
        query_dataset(args, host)


def remove_dataset(args: argparse.Namespace, host: str) -> JobId:
    hca = DatasetManager(environment=args.env, data_repo_client=get_api_client(host=host))
    return hca.delete_dataset(dataset_name=args.dataset_name, dataset_id=args.dataset_id)


def create_dataset(args: argparse.Namespace, host: str) -> JobId:
    hca = DatasetManager(environment=args.env, data_repo_client=get_api_client(host=host))
    return hca.create_dataset(
        dataset_name=args.dataset_name,
        billing_profile_id=args.billing_profile_id,
        schema_path=args.schema_path
    )


@dataclass
class DatasetManager:
    environment: str
    data_repo_client: RepositoryApi

    def create_dataset(
            self,
            dataset_name: str,
            billing_profile_id: str,
            schema_path: str,
            description: Optional[str] = None) -> JobId:
        """
        Creates a dataset in the data repo.
        :param dataset_name:  Name of the dataset
        :param billing_profile_id: GCP billing profile ID
        :param schema_path: Local path to a file containing a schema for the dataset
        :param description: Optional description for the dataset
        :return: Job ID of the dataset creation job
        """
        with open(schema_path, "r") as f:
            # verify that the schema is valid json
            parsed_schema = json.load(f)
            response = self.data_repo_client.create_dataset(
                dataset={
                    "name": dataset_name,
                    "description": description,
                    "defaultProfileId": billing_profile_id,
                    "schema": parsed_schema
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


def query_dataset(args: argparse.Namespace, host: str) -> None:
    hca = HcaManage(environment=args.env, data_repo_client=get_api_client(host=host), dataset=args.dataset_name)
    print(hca.enumerate_dataset())
