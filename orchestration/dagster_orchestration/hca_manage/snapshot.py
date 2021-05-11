import argparse
from dataclasses import dataclass, field
from datetime import datetime, date
import logging
from typing import Optional

from data_repo_client import RepositoryApi, SnapshotRequestModel, SnapshotRequestContentsModel

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, data_repo_profile_ids, DefaultHelpParser, JobId, get_api_client,\
    query_yes_no


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run(arguments: Optional[list[str]] = None) -> None:
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
        if query_yes_no("This will create a snapshot. Are you sure?"):
            create_snapshot(args, host)


def create_snapshot(args: argparse.Namespace, host: str) -> JobId:
    profile_id = data_repo_profile_ids[args.env]
    hca = SnapshotManager(
        environment=args.env,
        dataset=args.dataset,
        data_repo_profile_id=profile_id,
        data_repo_client=get_api_client(host=host))
    return hca.submit_snapshot_request(qualifier=args.qualifier)


def remove_snapshot(args: argparse.Namespace, host: str) -> JobId:
    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    return hca.delete_snapshot(snapshot_name=args.snapshot_name, snapshot_id=args.snapshot_id)


@dataclass
class SnapshotManager:
    environment: str
    data_repo_client: RepositoryApi
    dataset: Optional[str] = None
    data_repo_profile_id: Optional[str] = None
    reader_list: list[str] = field(init=False)

    def __post_init__(self) -> None:
        self.reader_list = {
            "dev": ["hca-snapshot-readers@dev.test.firecloud.org"],
            "prod": ["hca-snapshot-readers@firecloud.org"]
        }[self.environment]

    def submit_snapshot_request(
        self,
        qualifier: Optional[str] = None,
        snapshot_date: Optional[date] = None,
    ) -> JobId:
        snapshot_date = snapshot_date or datetime.today().date()
        return self.submit_snapshot_request_with_name(self.snapshot_name(qualifier, snapshot_date))

    def submit_snapshot_request_with_name(self, snapshot_name: str) -> JobId:
        """
        Submit a snapshot creation request.
        :param snapshot_name: name of snapshot to create
        :return: Job ID of the snapshot creation job
        """

        snapshot_request = SnapshotRequestModel(
            name=snapshot_name,
            profile_id=self.data_repo_profile_id,
            description=f"Create snapshot {snapshot_name}",
            contents=[SnapshotRequestContentsModel(dataset_name=self.dataset, mode="byFullView")],
            readers=self.reader_list
        )

        logging.info(snapshot_request)

        response = self.data_repo_client.create_snapshot(
            snapshot=snapshot_request
        )

        logging.info(f"Snapshot creation job id: {response.id}")
        return JobId(response.id)

    def snapshot_name(self, qualifier: Optional[str] = None, snapshot_date: Optional[date] = None) -> str:
        snapshot_date = snapshot_date or datetime.today().date()
        date_stamp = str(snapshot_date).replace("-", "")
        if qualifier:
            # prepend an underscore if this string is present
            qualifier = f"_{qualifier}"

        return f"{self.dataset}___{date_stamp}{qualifier}"

    def delete_snapshot(self, snapshot_name: Optional[str] = None, snapshot_id: Optional[str] = None) -> JobId:
        """
        Submit a snapshot deletion request. Requires either a snapshot ID or a snapshot name.
        :param snapshot_id: ID of the snapshot to delete
        :param snapshot_name: Name of the snapshot to delete.
        :return: Job ID of the snapshot creation job
        """

        if snapshot_name and not snapshot_id:
            response = self.data_repo_client.enumerate_snapshots(filter=snapshot_name)
            try:
                snapshot_id = response.items[0].id
            except IndexError:
                raise ValueError("The provided snapshot name returned no results.")
        elif snapshot_id and not snapshot_name:
            pass  # let snapshot_id argument pass through
        else:
            # can't have both/neither provided
            raise ValueError("You must provide either snapshot_name or snapshot_id, and cannot provide neither/both.")
        job_id: JobId = self.data_repo_client.delete_snapshot(snapshot_id).id
        logging.info(f"Snapshot deletion job id: {job_id}")
        return job_id
