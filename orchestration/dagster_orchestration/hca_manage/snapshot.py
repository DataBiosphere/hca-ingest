import argparse
from typing import List, Optional

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, data_repo_profile_ids, DefaultHelpParser, get_api_client, query_yes_no
from hca_manage.manage import HcaManage, JobId


def run(arguments: Optional[List[str]] = None) -> None:
    parser = DefaultHelpParser(description="A simple CLI to manage TDR snapshots.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target", choices=["dev", "prod"], required=True)

    snapshot_flags = parser.add_mutually_exclusive_group(required=True)
    snapshot_flags.add_argument("-c", "--create", help="Flag to create a snapshot", action="store_true")
    snapshot_flags.add_argument("-r", "--remove", help="Flag to delete a snapshot", action="store_true")

    # create
    snapshot_create_args = parser.add_argument_group()
    snapshot_create_args.add_argument("-d", "--dataset", help="The Jade dataset to target")
    snapshot_create_args.add_argument("-q", "--qualifier", help="Optional qualifier to append to the snapshot name")

    # delete
    snapshot_delete_args = parser.add_mutually_exclusive_group(required=True)
    snapshot_delete_args.add_argument("-n", "--snapshot_name", help="Name of snapshot to delete.")
    snapshot_delete_args.add_argument("-i", "--snapshot_id", help="ID of snapshot to delete.")

    args = parser.parse_args(arguments)
    host = data_repo_host[args.env]

    if args.remove:
        if query_yes_no("Are you sure?"):
            remove_snapshot(args, host)
        else:
            # TODO change to logger?
            print("No deletion attempted.")
    elif args.create:
        if query_yes_no("This will create a dataset. Are you sure?"):
            create_snapshot(args, host)


def create_snapshot(args: argparse.Namespace, host: str) -> JobId:
    profile_id = data_repo_profile_ids[args.env]
    hca = HcaManage(
        environment=args.env,
        dataset=args.dataset,
        data_repo_profile_id=profile_id,
        data_repo_client=get_api_client(host=host))
    return hca.submit_snapshot_request(qualifier=args.qualifier)


def remove_snapshot(args: argparse.Namespace, host: str) -> JobId:
    hca = HcaManage(environment=args.env, data_repo_client=get_api_client(host=host))
    return hca.delete_snapshot(snapshot_name=args.snapshot_name, snapshot_id=args.snapshot_id)
