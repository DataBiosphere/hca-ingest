import sys
import argparse

from data_repo_client import ApiClient, Configuration, RepositoryApi

from hca_manage import __version__ as hca_manage_version
from .manage import HcaManage
from hca_orchestration.resources.base import default_google_access_token

data_repo_host = {
    "dev": "https://jade.datarepo-dev.broadinstitute.org/",
    "prod": "https://jade-terra.datarepo-prod.broadinstitute.org/"
}
data_repo_profile_ids = {
    "dev": "390e7a85-d47f-4531-b612-165fc977d3bd",
    "prod": "db61c343-6dfe-4d14-84e9-60ddf97ea73f"
}


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

    # validation checks
    parser_check = subparsers.add_parser("check",
                                         help="Command to check HCA datasets for duplicates and null file references")
    # only allow if env is prod
    parser_check.add_argument("-p", "--project", help="The Jade project to target, defaults to correct project for dev")
    parser_check.add_argument("-d", "--dataset", help="The Jade dataset to target", required=True)
    parser_check.add_argument("-r", "--remove",
                              help="Remove problematic rows. If flag not set, will only check for presence of problematic rows",
                              action="store_true")

    # snapshot management
    parser_snapshot = subparsers.add_parser("snapshot", help="Command to manage snapshots")
    snapshot_flags = parser_snapshot.add_mutually_exclusive_group(required=True)
    snapshot_flags.add_argument("-c", "--create", help="Flag to indicate snapshot creation", action="store_true")
    snapshot_flags.add_argument("-r", "--remove", help="Flag to indicate snapshot deletion", action="store_true")
    snapshot_delete_args = parser_snapshot.add_mutually_exclusive_group(required=True)
    snapshot_delete_args.add_argument("-n", "--snapshot_name", help="Name of snapshot to delete.")
    snapshot_delete_args.add_argument("-i", "--snapshot_id", help="ID of snapshot to delete.")
    snapshot_create_args = parser_snapshot.add_argument_group()
    snapshot_create_args.add_argument("-d", "--dataset", help="The Jade dataset to target")
    snapshot_create_args.add_argument("-q", "--qualifier", help="Optional qualifier to append to the snapshot name")

    # dataset management
    parser_dataset = subparsers.add_parser("dataset", help="Command to manage datasets")
    dataset_flags = parser_dataset.add_mutually_exclusive_group(required=True)
    dataset_flags.add_argument("-r", "--remove", help="Flag to indicate dataset deletion", action="store_true")
    dataset_delete_args = parser_dataset.add_mutually_exclusive_group(required=True)
    dataset_delete_args.add_argument("-n", "--dataset_name", help="Name of dataset to delete.")
    dataset_delete_args.add_argument("-i", "--dataset_id", help="ID of dataset to delete.")

    args = parser.parse_args(arguments)

    host = data_repo_host[args.env]

    if args.command == "check":
        check_data(args, host, parser)
    elif args.command == "snapshot":
        if args.create:
            create_snapshot(args, host)
        elif args.remove:
            if query_yes_no("Are you sure?"):
                remove_snapshot(args, host)
            else:
                print("No deletes attempted.")
    elif args.command == "dataset":
        if args.remove:
            if query_yes_no("Are you sure?"):
                remove_dataset(args, host)
            else:
                print("No deletes attempted.")


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
    profile_id = data_repo_profile_ids[args.env]
    hca = HcaManage(
        environment=args.env,
        dataset=args.dataset,
        data_repo_profile_id=profile_id,
        data_repo_client=get_api_client(host=host))
    return hca.submit_snapshot_request(qualifier=args.qualifier)


def remove_snapshot(args, host):
    hca = HcaManage(environment=args.env, data_repo_client=get_api_client(host=host))
    return hca.delete_snapshot(snapshot_name=args.snapshot_name, snapshot_id=args.snapshot_id)


def remove_dataset(args, host):
    hca = HcaManage(environment=args.env, data_repo_client=get_api_client(host=host))
    return hca.delete_dataset(dataset_name=args.dataset_name, dataset_id=args.dataset_id)


def query_yes_no(question, default="no"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError(f"invalid default answer: '{default}'")

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")
