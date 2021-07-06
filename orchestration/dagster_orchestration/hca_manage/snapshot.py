import argparse
from dataclasses import dataclass, field
from datetime import datetime, date
import logging
from re import search
import sys
from typing import Optional

from data_repo_client import RepositoryApi, SnapshotRequestModel, SnapshotRequestContentsModel, EnumerateSnapshotModel
from dagster_utils.contrib.data_repo.jobs import poll_job, JobPollException
from dagster_utils.contrib.data_repo.typing import JobId

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, data_repo_profile_ids, DefaultHelpParser, get_api_client, \
    query_yes_no, tdr_operation, setup_cli_logging_format

MAX_SNAPSHOT_DELETE_POLL_SECONDS = 120
SNAPSHOT_DELETE_POLL_INTERVAL_SECONDS = 2
SNAPSHOT_NAME_REGEX = r"^hca_(dev|prod|staging)_(\d{4})(\d{2})(\d{2})(_[a-zA-Z][a-zA-Z0-9]{0,13})?_([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})?__(\d{4})(\d{2})(\d{2})(?:_([a-zA-Z][a-zA-Z0-9]{0,13}))?$"


class InvalidSnapshotNameException(ValueError):
    pass


def run(arguments: Optional[list[str]] = None) -> None:
    setup_cli_logging_format()
    parser = DefaultHelpParser(description="A simple CLI to manage TDR snapshots.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument(
        "-e",
        "--env",
        help="The Jade environment to target",
        choices=data_repo_host.keys(),
        required=True)
    subparsers = parser.add_subparsers()

    # create
    snapshot_create = subparsers.add_parser("create")
    snapshot_create.add_argument("-d", "--dataset", help="The Jade dataset to target")
    snapshot_create.add_argument("-q", "--qualifier", help="Optional qualifier to append to the snapshot name")
    snapshot_create.set_defaults(func=_create_snapshot)

    # remove
    snapshot_delete = subparsers.add_parser("remove")
    snapshot_delete.add_argument("-n", "--snapshot_name", help="Name of snapshot to delete.")
    snapshot_delete.add_argument("-i", "--snapshot_id", help="ID of snapshot to delete.")
    snapshot_delete.set_defaults(func=_remove_snapshot)

    snapshot_query = subparsers.add_parser("query")
    snapshot_query.add_argument("-n", "--snapshot_name", help="Name of snapshot to filter for")
    snapshot_query.set_defaults(func=_query_snapshot)

    args = parser.parse_args(arguments)
    args.func(args)


@tdr_operation
def _create_snapshot(args: argparse.Namespace) -> None:
    if not query_yes_no("Are you sure?"):
        return

    host = data_repo_host[args.env]
    profile_id = data_repo_profile_ids[args.env]
    hca = SnapshotManager(
        environment=args.env,
        dataset=args.dataset,
        data_repo_profile_id=profile_id,
        data_repo_client=get_api_client(host=host))
    hca.submit_snapshot_request(qualifier=args.qualifier)


@tdr_operation
def _remove_snapshot(args: argparse.Namespace) -> None:
    if not query_yes_no("Are you sure?"):
        return

    host = data_repo_host[args.env]
    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    hca.delete_snapshot(snapshot_name=args.snapshot_name, snapshot_id=args.snapshot_id)


@tdr_operation
def _query_snapshot(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]

    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    logging.info(hca.query_snapshot(snapshot_name=args.snapshot_name))


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
            "prod": ["hca-snapshot-readers@firecloud.org", "monster@firecloud.org"]
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
        if not search(SNAPSHOT_NAME_REGEX, snapshot_name):
            raise InvalidSnapshotNameException(f"Snapshot name {snapshot_name} is invalid")

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
        if not qualifier:
            qualifier = ""
        else:
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
        try:
            poll_job(
                job_id,
                MAX_SNAPSHOT_DELETE_POLL_SECONDS,
                SNAPSHOT_DELETE_POLL_INTERVAL_SECONDS,
                self.data_repo_client
            )
        except JobPollException:
            job_result = self.data_repo_client.retrieve_job_result(job_id)
            logging.error("Delete Snapshot failed, results =")
            logging.error(job_result)
            sys.exit(1)
        return job_id

    def query_snapshot(self, snapshot_name: Optional[str] = None) -> EnumerateSnapshotModel:
        return self.data_repo_client.enumerate_snapshots(filter=snapshot_name)


if __name__ == '__main__':
    run()
