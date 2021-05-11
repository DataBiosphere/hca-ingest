import argparse
from dataclasses import dataclass
import logging
from typing import Optional

from data_repo_client import RepositoryApi

from hca_manage import __version__ as hca_manage_version
from hca_manage.bq_managers import DanglingFileRefManager, DuplicatesManager, NullFileRefManager
from hca_manage.common import DefaultHelpParser, ProblemCount, data_repo_host, get_api_client, query_yes_no
from hca_manage.soft_delete import SoftDeleteManager


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run(arguments: Optional[list[str]] = None) -> None:
    parser = DefaultHelpParser(description="A simple CLI to check for issues in a TDR dataset.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target", choices=["dev", "prod"], required=True)

    parser.add_argument("-p", "--project", help="The Jade project to target", required=True)
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

    hca = CheckManager(environment=args.env,
                       project=project,
                       dataset=args.dataset,
                       data_repo_client=get_api_client(host))

    if remove:
        hca.remove_all()
    else:
        hca.check_for_all()


@dataclass
class CheckManager:
    environment: str
    project: str
    dataset: str
    data_repo_client: RepositoryApi

    @property
    def soft_delete_manager(self) -> SoftDeleteManager:
        return SoftDeleteManager(environment=self.environment,
                                 dataset=self.dataset,
                                 project=self.project,
                                 data_repo_client=self.data_repo_client)

    @property
    def duplicate_manager(self) -> DuplicatesManager:
        return DuplicatesManager(dataset=self.dataset,
                                 project=self.project,
                                 soft_delete_manager=self.soft_delete_manager)

    @property
    def null_file_ref_manager(self) -> NullFileRefManager:
        return NullFileRefManager(dataset=self.dataset,
                                  project=self.project,
                                  soft_delete_manager=self.soft_delete_manager)

    @property
    def dangling_file_ref_manager(self) -> DanglingFileRefManager:
        return DanglingFileRefManager(dataset=self.dataset,
                                      project=self.project,
                                      soft_delete_manager=self.soft_delete_manager)

    def check_for_all(self) -> ProblemCount:
        """
        Check and print the number of duplicates and null file references in all tables in the dataset.
        :return: A named tuple with the counts of rows to soft delete
        """
        logging.info("Processing...")
        duplicate_count = self.duplicate_manager.check_or_delete_rows()
        null_file_ref_count = self.null_file_ref_manager.check_or_delete_rows()
        dangling_proj_refs_count = self.dangling_file_ref_manager.check_or_delete_rows()
        logging.info("Finished.")
        return ProblemCount(
            duplicates=duplicate_count,
            null_file_refs=null_file_ref_count,
            dangling_project_refs=dangling_proj_refs_count
        )

    def remove_all(self) -> ProblemCount:
        """
        Check and print the number of duplicates and null file references for each table in the dataset, then soft
        delete the problematic rows.
        :return: A named tuple with the counts of rows to soft delete
        """
        logging.info("Processing, deleting as we find anything...")
        duplicate_count = self.duplicate_manager.check_or_delete_rows(soft_delete=True)
        null_file_ref_count = self.null_file_ref_manager.check_or_delete_rows(soft_delete=True)
        logging.info("Skipping any rows with dangling project refs, manual intervention required")
        logging.info("Finished.")
        return ProblemCount(
            duplicates=duplicate_count,
            null_file_refs=null_file_ref_count,
            dangling_project_refs=0
        )
