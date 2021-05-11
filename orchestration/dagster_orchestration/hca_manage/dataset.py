import argparse
from typing import Optional

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, DefaultHelpParser, get_api_client, query_yes_no
from hca_manage.manage import HcaManage, JobId


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
    hca = HcaManage(environment=args.env, data_repo_client=get_api_client(host=host))
    return hca.delete_dataset(dataset_name=args.dataset_name, dataset_id=args.dataset_id)


def create_dataset(args: argparse.Namespace, host: str) -> JobId:
    hca = HcaManage(environment=args.env, data_repo_client=get_api_client(host=host))
    return hca.create_dataset(
        dataset_name=args.dataset_name,
        billing_profile_id=args.billing_profile_id,
        schema_path=args.schema_path
    )


def query_dataset(args: argparse.Namespace, host: str) -> None:
    hca = HcaManage(environment=args.env, data_repo_client=get_api_client(host=host), dataset=args.dataset_name)
    print(hca.enumerate_dataset())
