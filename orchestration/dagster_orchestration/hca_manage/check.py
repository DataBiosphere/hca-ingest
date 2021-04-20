import argparse
from typing import List, Optional

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, DefaultHelpParser, get_api_client, query_yes_no
from hca_manage.manage import HcaManage


def run(arguments: Optional[List[str]] = None) -> None:
    parser = DefaultHelpParser(description="A simple CLI to check for issues in a TDR dataset.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target", choices=["dev", "prod"], required=True)

    parser.add_argument("-p", "--project", help="The Jade project to target, defaults to correct project for dev")
    parser.add_argument("-d", "--dataset", help="The Jade dataset to target", required=True)
    parser.add_argument("-r", "--remove",
                        help="Remove problematic rows. If flag not set, "
                             "will only check for presence of problematic rows",
                        action="store_true")

    args = parser.parse_args(arguments)
    host = data_repo_host[args.env]

    if args.remove:
        if query_yes_no("Are you sure?"):
            check_data(args, host, parser, remove=True)
        else:
            print("No removal attempted.")
    else:
        check_data(args, host, parser)


def check_data(args: argparse.Namespace, host: str, parser: argparse.ArgumentParser, remove: bool = False) -> None:
    if args.env == "dev":
        if args.project:
            parser.error("Do not specify a project when the environment is dev, there is only one project.")
        project = "broad-jade-dev-data"
    else:
        project = args.project

    hca = HcaManage(environment=args.env,
                    project=project,
                    dataset=args.dataset,
                    data_repo_client=get_api_client(host=host))

    if remove:
        hca.remove_all()
    else:
        hca.check_for_all()
