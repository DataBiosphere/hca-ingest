import sys
import argparse

from hca_utils import __version__ as hca_utils_version
from .utils import HcaUtils


class DefaultHelpParser(argparse.ArgumentParser):
    def error(self, message):
        """Print help message by default."""
        sys.stderr.write(f'error: {message}\n')
        self.print_help()
        sys.exit(2)


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
    else:
        project = args.project

    if not sys.argv[1:]:
        parser.error("No commands or arguments provided!")

    hca = HcaUtils(environment=args.env, project=project, dataset=args.dataset)

    if args.remove:
        hca.remove_all()
    else:
        hca.check_for_all()
