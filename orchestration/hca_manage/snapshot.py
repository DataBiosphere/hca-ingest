import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from re import search
from typing import Optional

from dagster_utils.contrib.data_repo.jobs import JobPollException, poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from dagster_utils.resources.sam import Sam
from data_repo_client import (
    EnumerateSnapshotModel,
    PolicyMemberRequest,
    PolicyResponse,
    RepositoryApi,
    SnapshotModel,
    SnapshotRequestContentsModel,
    SnapshotRequestModel,
)

# isort: skip

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import (
    DefaultHelpParser,
    data_repo_host,
    data_repo_profile_ids,
    get_api_client,
    query_yes_no,
    sam_host,
    setup_cli_logging_format,
    tdr_operation,
)

MAX_SNAPSHOT_DELETE_POLL_SECONDS = 120
SNAPSHOT_DELETE_POLL_INTERVAL_SECONDS = 2
LEGACY_SNAPSHOT_NAME_REGEX = r"^(hca|lungmap)_(dev|prod|staging)_(\d{4})(\d{2})(\d{2})(_[a-zA-Z][a-zA-Z0-9]{0,13})?_([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})?__(\d{4})(\d{2})(\d{2})(?:_([a-zA-Z][a-zA-Z0-9]{0,13}))?$"  # noqa: E501
UPDATED_SNAPSHOT_NAME_REGEX = r"^(hca|lungmap)_(dev|prod|staging)_([0-9a-f]{32})?__(\d{4})(\d{2})(\d{2})(?:_([a-zA-Z][a-zA-Z0-9]{0,15}))?_(\d{4})(\d{2})(\d{2})(?:_([a-zA-Z][a-zA-Z0-9]{0,15}))?$"  # noqa: E501


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
    snapshot_create.add_argument(
        "-v",
        "--validate_snapshot_name",
        help="Skip snapshot name validation",
        action=argparse.BooleanOptionalAction,
        default=True)
    snapshot_create.set_defaults(func=_create_snapshot)

    # remove
    snapshot_delete = subparsers.add_parser("remove")
    snapshot_delete.add_argument("-n", "--snapshot_name", help="Name of snapshot to delete.")
    snapshot_delete.add_argument("-i", "--snapshot_id", help="ID of snapshot to delete.")
    snapshot_delete.set_defaults(func=_remove_snapshot)

    # get id
    snapshot_get_id = subparsers.add_parser("get_id")
    snapshot_get_id.add_argument("-n", "--snapshot_name", help="Name of snapshot to get id for.")
    snapshot_get_id.set_defaults(func=_get_snapshot_id)

    snapshot_query = subparsers.add_parser("query")
    snapshot_query.add_argument("-n", "--snapshot_name", help="Name of snapshot to filter for")
    snapshot_query.set_defaults(func=_query_snapshot)

    snapshot_add_policy_member = subparsers.add_parser("add_policy_member")
    snapshot_add_policy_member.add_argument(
        "-p",
        "--policy_member",
        help="Email address of user to add as a policy member",
        required=True)
    snapshot_add_policy_member.add_argument("-i", "--snapshot_id", help="Id of snapshot", required=True)
    snapshot_add_policy_member.add_argument(
        "-n",
        "--policy_name",
        help="Name of policy (one of steward, reader, discoverer)",
        required=True)
    snapshot_add_policy_member.set_defaults(func=_add_policy_member)

    # retrieve policies
    snapshot_retrieve_policies = subparsers.add_parser("retrieve_snapshot_policies")
    snapshot_retrieve_policies.add_argument("-i", "--snapshot_id", help="Id of snapshot", required=True)
    snapshot_retrieve_policies.set_defaults(func=_retrieve_policies)

    # retrieve snapshot
    snapshot_retrieve = subparsers.add_parser("retrieve")
    snapshot_retrieve.add_argument("-i", "--snapshot_id", help="Id of snapshot", required=True)
    snapshot_retrieve.set_defaults(func=_retrieve_snapshot)

    # set public
    snapshot_public = subparsers.add_parser("mark_public")
    snapshot_public.add_argument("-i", "--snapshot_id", help="Id of snapshot", required=True)
    snapshot_public.set_defaults(func=_mark_snapshot_public)

    # set private
    snapshot_public = subparsers.add_parser("mark_private")
    snapshot_public.add_argument("-i", "--snapshot_id", help="Id of snapshot", required=True)
    snapshot_public.set_defaults(func=_mark_snapshot_private)

    # bulk add policy member
    snapshot_bulk_add_policy_member = subparsers.add_parser("bulk_add_policy_member")
    snapshot_bulk_add_policy_member.add_argument(
        "-p",
        "--policy_member",
        help="Email address of user to add as a policy member",
        required=True)
    snapshot_bulk_add_policy_member.add_argument("-s", "--snapshot_name", help="Name of snapshot(s)", required=True)
    snapshot_bulk_add_policy_member.add_argument(
        "-n",
        "--policy_name",
        help="Name of policy (one of steward, reader, discoverer)",
        required=True)
    snapshot_bulk_add_policy_member.set_defaults(func=_bulk_add_policy_member)

    args = parser.parse_args(arguments)
    args.func(args)


@tdr_operation
def _bulk_add_policy_member(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]

    policy_member = args.policy_member
    snapshot_name = args.snapshot_name
    policy_name = args.policy_name

    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    hca.bulk_add_policy_member(policy_member, policy_name, snapshot_name)


@tdr_operation
def _mark_snapshot_public(args: argparse.Namespace) -> None:
    snapshot_id = args.snapshot_id

    sam_client = Sam(base_url=sam_host[args.env])

    if not query_yes_no(f"This will set snapshot id {snapshot_id} to public, are you sure?"):
        return

    sam_client.set_public_flag(snapshot_id, True)


@tdr_operation
def _mark_snapshot_private(args: argparse.Namespace) -> None:
    snapshot_id = args.snapshot_id

    sam_client = Sam(base_url=sam_host[args.env])

    if not query_yes_no(f"This will set snapshot id {snapshot_id} to private, are you sure?"):
        return

    sam_client.set_public_flag(snapshot_id, False)


@tdr_operation
def _retrieve_snapshot(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]

    snapshot_id = args.snapshot_id
    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    response = hca.retrieve_snapshot(snapshot_id=snapshot_id)
    logging.info(response)


@tdr_operation
def _retrieve_policies(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]

    snapshot_id = args.snapshot_id
    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    response = hca.retrieve_policies(snapshot_id=snapshot_id)
    logging.info(response)


@tdr_operation
def _add_policy_member(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]

    policy_member = args.policy_member
    snapshot_id = args.snapshot_id
    policy_name = args.policy_name

    if not query_yes_no(
            f"This will add {policy_member} as a {policy_name} to snapshot id = {snapshot_id}, are you sure?"):
        return

    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    response = hca.add_policy_member(policy_member, policy_name, snapshot_id)
    logging.info(response)


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
    hca.submit_snapshot_request(qualifier=args.qualifier, validate_snapshot_name=args.validate_snapshot_name)


@tdr_operation
def _remove_snapshot(args: argparse.Namespace) -> None:
    if not query_yes_no("Are you sure?"):
        return

    host = data_repo_host[args.env]
    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    hca.delete_snapshot(snapshot_name=args.snapshot_name, snapshot_id=args.snapshot_id)


@tdr_operation
def _get_snapshot_id(args:argparse.Namespace) -> None:
    host = data_repo_host[args.env]
    hca = SnapshotManager(environment=args.env, data_repo_client=get_api_client(host=host))
    hca.get_snapshot_id(snapshot_name=args.snapshot_name)


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
        self.managed_access_reader_list = {
            "dev": [
                "hca-snapshot-readers@dev.test.firecloud.org",
                "monster-dev@dev.test.firecloud.org",
                "azul-dev@dev.test.firecloud.org"
            ],
            "prod": ["hca-snapshot-readers@firecloud.org", "monster@firecloud.org"],
            "real_prod": []
        }[self.environment]
        self.public_access_reader_list = {
            "dev": [
                "hca-snapshot-readers@dev.test.firecloud.org",
                "monster-dev@dev.test.firecloud.org",
            ],
            "prod": ["hca-snapshot-readers@firecloud.org", "monster@firecloud.org"],
            "real_prod": []
        }[self.environment]

    def submit_snapshot_request(
            self,
            qualifier: Optional[str] = None,
            snapshot_date: Optional[date] = None,
            validate_snapshot_name: bool = True
    ) -> JobId:
        snapshot_date = snapshot_date or datetime.today().date()
        return self.submit_snapshot_request_with_name(
            self.snapshot_name(qualifier, snapshot_date),
            validate_snapshot_name=validate_snapshot_name
        )

    def _validate_snapshot_name(self, snapshot_name: str) -> None:
        if not search(LEGACY_SNAPSHOT_NAME_REGEX, snapshot_name) \
                and not search(UPDATED_SNAPSHOT_NAME_REGEX, snapshot_name):
            raise InvalidSnapshotNameException(f"Snapshot name {snapshot_name} is invalid")

    def submit_snapshot_request_with_name(
            self,
            snapshot_name: str,
            managed_access: bool = False,
            validate_snapshot_name: bool = True
    ) -> JobId:
        """
        Submit a snapshot creation request.
        :param snapshot_name: name of snapshot to created
        :param managed_access: Determine which set of readers to grant access to this snapshot (default = False)
        :param validate_snapshot_name: Validate the submitted snapshot name against the DCP2 specification naming regex
        :return: Job ID of the snapshot creation job
        """
        if validate_snapshot_name:
            self._validate_snapshot_name(snapshot_name)

        reader_list = self.managed_access_reader_list if managed_access else self.public_access_reader_list
        snapshot_request = SnapshotRequestModel(
            name=snapshot_name,
            profile_id=self.data_repo_profile_id,
            description=f"Create snapshot {snapshot_name}",
            contents=[SnapshotRequestContentsModel(dataset_name=self.dataset, mode="byFullView")],
            readers=reader_list
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

    def get_snapshot_id(self, snapshot_name: str) -> str:
        response = self.data_repo_client.enumerate_snapshots(filter=snapshot_name)
        try:
            snapshot_id = response.items[0].id
        except IndexError:
            raise ValueError("The provided snapshot name returned no results.")
        return snapshot_id

    def query_snapshot(
            self,
            snapshot_name: Optional[str] = None,
            result_limit: Optional[int] = 1000
    ) -> EnumerateSnapshotModel:
        return self.data_repo_client.enumerate_snapshots(filter=snapshot_name, limit=result_limit)

    def add_policy_member(self, policy_member: str, policy_name: str, snapshot_id: str) -> PolicyResponse:
        payload = PolicyMemberRequest(email=policy_member)
        return self.data_repo_client.add_snapshot_policy_member(snapshot_id, policy_name, policy_member=payload)

    def retrieve_policies(self, snapshot_id: str) -> PolicyResponse:
        return self.data_repo_client.retrieve_snapshot_policies(id=snapshot_id)

    def retrieve_snapshot(self, snapshot_id: str) -> SnapshotModel:
        return self.data_repo_client.retrieve_snapshot(id=snapshot_id, include=["PROFILE,DATA_PROJECT"])

    def bulk_add_policy_member(self, policy_member: str, policy_name: str, snapshot_name_filter: str) -> None:
        snapshots = self.query_snapshot(snapshot_name_filter)
        if not snapshots.items:
            logging.error(f"No snapshots found for filter {snapshot_name_filter}")
            return

        logging.info(
            f"Adding {policy_member} as a {policy_name} to {len(snapshots.items)}"\
            f"snapshots matching filter {snapshot_name_filter}"
        )
        for snapshot in snapshots.items:
            payload = PolicyMemberRequest(email=policy_member)
            self.data_repo_client.add_snapshot_policy_member(snapshot.id, policy_name, policy_member=payload)
            logging.info(f"Added {policy_member} as a {policy_name} to {snapshot.name} ({snapshot.id})")


if __name__ == '__main__':
    run()
