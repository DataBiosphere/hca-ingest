import sys
import argparse

from data_repo_client import ApiClient, Configuration, RepositoryApi

from hca_manage import __version__ as hca_manage_version
from .manage import HcaManage
from hca_orchestration.resources.base import default_google_access_token


class DefaultHelpParser(argparse.ArgumentParser):
    def error(self, message):
        """Print help message by default."""
        sys.stderr.write(f'error: {message}\n')
        self.print_help()
        sys.exit(2)


def get_api_client(host: str) -> RepositoryApi:
    # create API client
    config = Configuration(host=host)
    config.access_token = default_google_access_token()
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    return RepositoryApi(api_client=client)


def run(arguments=None):
    parser = DefaultHelpParser(description="A simple HCA Management CLI.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target, defaults to dev", choices=["dev", "prod"],
                        required=True)
    subparsers = parser.add_subparsers(dest='command')

    parser_check = subparsers.add_parser("check",
                                         help="Command to check HCA datasets for duplicates and null file references")
    # only allow if env is prod
    parser_check.add_argument("-p", "--project", help="The Jade project to target, defaults to correct project for dev")
    parser_check.add_argument("-d", "--dataset", help="The Jade dataset to target", required=True)
    parser_check.add_argument("-r", "--remove",
                              help="Remove problematic rows. If flag not set, will only check for presence of problematic rows.",
                              action="store_true")

    parser_snapshot = subparsers.add_parser("snapshot", help="Command to create a snapshot for an HCA release")
    parser_snapshot.add_argument("-d", "--dataset", help="The Jade dataset to target", required=True)
    parser_snapshot.add_argument("-q", "--qualifier", help="Optional qualifier to append to the snapshot name")

    args = parser.parse_args(arguments)

    host = "https://jade-terra.datarepo-prod.broadinstitute.org/"
    if args.env == "dev":
        host = "https://jade.datarepo-dev.broadinstitute.org/"

    if args.command == "check":
        check_data(args, host, parser)
    elif args.command == "snapshot":
        create_snapshot(args, host)


def check_data(args, host, parser):
    if args.env == "dev":
        if args.project:
            parser.error("Do not specify a project when the environment is dev, there is only one project.")
        project = "broad-jade-dev-data"
    else:
        project = args.project

    if not sys.argv[1:]:
        parser.error("No commands or arguments provided!")

    hca = HcaManage(environment=args.env,
                    project=project,
                    dataset=args.dataset,
                    data_repo_client=get_api_client(host=host))

    if args.remove:
        hca.remove_all()
    else:
        hca.check_for_all()


def create_snapshot(args, host):
    hca = HcaManage(environment=args.env,
                    project=None,
                    dataset=args.dataset,
                    data_repo_client=get_api_client(host=host))
    hca.submit_snapshot_request(optional_qualifier=args.qualifier)
