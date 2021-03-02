import sys
import argparse

from data_repo_client import ApiClient, Configuration, RepositoryApi
import google.auth
from google.auth.transport.requests import Request

from hca_utils import __version__ as hca_utils_version
from .utils import HcaUtils


class DefaultHelpParser(argparse.ArgumentParser):
    def error(self, message):
        """Print help message by default."""
        sys.stderr.write(f'error: {message}\n')
        self.print_help()
        sys.exit(2)


def get_api_client(host: str) -> RepositoryApi:
    # get token for jade, assumes application default credentials work for specified environment
    credentials, _ = google.auth.default()
    credentials.refresh(Request())

    # create API client
    config = Configuration(host=host)
    config.access_token = credentials.token
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    return RepositoryApi(api_client=client)


def run(arguments=None):
    parser = DefaultHelpParser(description="A simple HcaUtils CLI.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_utils_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target, defaults to dev", choices=["dev", "prod"],
                        required=True)
    # only allow if env is prod
    parser.add_argument("-p", "--project", help="The Jade project to target, defaults to correct project for dev")
    parser.add_argument("-d", "--dataset", help="The Jade dataset to target", required=True)
    parser.add_argument("-r", "--remove",
                        help="Remove problematic rows. If flag not set, will only check for presence of problematic rows.",
                        action="store_true")

    args = parser.parse_args(arguments)
    if args.env == "dev":
        if args.project:
            parser.error("Do not specify a project when the environment is dev, there is only one project.")
        project = "broad-jade-dev-data"
        host = "https://jade.datarepo-dev.broadinstitute.org/"
    else:
        project = args.project
        host = "https://jade-terra.datarepo-prod.broadinstitute.org/"

    if not sys.argv[1:]:
        parser.error("No commands or arguments provided!")

    hca = HcaUtils(environment=args.env, project=project, dataset=args.dataset,
                   data_repo_client=get_api_client(host=host))

    if args.remove:
        hca.remove_all()
    else:
        hca.check_for_all()
