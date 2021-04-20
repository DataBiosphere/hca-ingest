import argparse
from typing import List, Optional

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, DefaultHelpParser, get_api_client, query_yes_no
from hca_manage.manage import HcaManage


def run(arguments: Optional[List[str]] = None) -> None:
    parser = DefaultHelpParser(description="A simple CLI to soft delete rows in a TDR dataset.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target", choices=["dev", "prod"], required=True)

    parser.add_argument("-p", "--path", help="Path to csv containing row IDs to soft delete")
    parser.add_argument("-t", "--target_table", help="Table containing the rows slated for soft deletion")
    parser.add_argument("-j", "--project", help="project")
    parser.add_argument("-d", "--dataset", help="dataset")

    args = parser.parse_args(arguments)
    host = data_repo_host[args.env]

    if query_yes_no("Are you sure?"):
        soft_delete(args, host)


def soft_delete(args: argparse.Namespace, host: str) -> None:
    hca = HcaManage(environment=args.env, data_repo_client=get_api_client(host=host),
                    project=args.project, dataset=args.dataset)
    hca.soft_delete_rows(args.path, args.target_table)
