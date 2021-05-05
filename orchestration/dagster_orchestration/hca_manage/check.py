import argparse
from dataclasses import dataclass
import logging
import os
from typing import Callable, Optional

from google.cloud import bigquery

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import DefaultHelpParser, ProblemCount, data_repo_host, get_api_client, populate_row_id_csv, \
    query_yes_no
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
                       data_repo_client=get_api_client(host=host))

    if remove:
        hca.remove_all()
    else:
        hca.check_for_all()


@dataclass
class CheckManager(SoftDeleteManager):

    @property
    def bigquery_client(self) -> bigquery.client.Client:
        return bigquery.Client(project=self.project)

    def check_for_all(self) -> ProblemCount:
        """
        Check and print the number of duplicates and null file references in all tables in the dataset.
        :return: A named tuple with the counts of rows to soft delete
        """
        logging.info("Processing...")
        duplicate_count = self.process_duplicates()
        null_file_ref_count = self.process_null_file_refs()
        dangling_proj_refs_count = self.process_dangling_proj_refs()
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
        duplicate_count = self.process_duplicates(soft_delete=True)
        null_file_ref_count = self.process_null_file_refs(soft_delete=True)
        logging.info("Skipping any rows with dangling project refs, manual intervention required")
        logging.info("Finished.")
        return ProblemCount(
            duplicates=duplicate_count,
            null_file_refs=null_file_ref_count,
            dangling_project_refs=0
        )

    def process_dangling_proj_refs(self, soft_delete: bool = False) -> int:
        """
        Check for any entities with project_id values that do not have a corresponding entry in the projects
        table
        :return: Number of rows with dangling project refs
        """
        if soft_delete:
            raise NotImplementedError("Soft deleting rows with dangling project refs is unsupported")

        def links_table() -> set[str]:
            return {'links'}

        return self._process_rows(links_table, self.get_dangling_proj_refs, soft_delete=soft_delete,
                                  issue="found rows with dangling project refs")

    def get_dangling_proj_refs(self, target_table: str) -> set[str]:
        query = f"""
        SELECT links.links_id
        FROM `{self.project}.datarepo_{self.dataset}.{target_table}` links
                 LEFT JOIN
             `{self.project}.datarepo_{self.dataset}.project` projects
             ON
                 {target_table}.project_id = projects.project_id
        WHERE projects.project_id IS NULL;
        """
        return self._hit_bigquery(query)

    def process_null_file_refs(self, soft_delete: bool = False) -> int:
        """
        Check/remove and print the number of null file references for each table in the dataset that has a `file_id`
        column.
        :return: Number of rows with null file refs to soft delete
        """
        return self._process_rows(self.get_file_table_names, self.get_null_filerefs, soft_delete=soft_delete,
                                  issue="null file refs")

    def get_file_table_names(self) -> set[str]:
        """
        Gets the table names for tables that have a `file_id` column.
        :return: A set of table names.
        """
        query = f"""
        WITH fileRefTables AS (
            SELECT *
            FROM `{self.project}.datarepo_{self.dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE column_name = "file_id"),
        desiredViews AS (
            SELECT * FROM `{self.project}.datarepo_{self.dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = "VIEW")
        SELECT desiredViews.table_name
        FROM fileRefTables
        JOIN desiredViews
          ON fileRefTables.table_name = desiredViews.table_name
        """

        return self._hit_bigquery(query)

    def get_null_filerefs(self, target_table: str) -> set[str]:
        """
        Determines what rows have null values in the file_id column. We want to soft delete those.
        :param target_table: The particular table to operate on.
        :return: A set of row ids to soft delete.
        """
        query = f"""
        SELECT datarepo_row_id
        FROM `{self.project}.datarepo_{self.dataset}.{target_table}` WHERE file_id IS NULL
        """

        return self._hit_bigquery(query)

    def process_duplicates(self, soft_delete: bool = False) -> int:
        """
        Check and print the number of duplicates for each table in the dataset.
        :return: Number of duplicate rows to soft delete
        """
        return self._process_rows(self.get_all_table_names, self.get_duplicates, soft_delete=soft_delete,
                                  issue="duplicate rows")

    def get_duplicates(self, target_table: str) -> set[str]:
        """
        Determines what rows are undesired duplicates. We want to soft delete everything but the latest version for a
        given entity_id.
        :param target_table: The particular table to operate on.
        :return: A set of row ids to soft delete.
        """
        sql_table = f"`{self.project}.datarepo_{self.dataset}.{target_table}`"

        # rid -> row_id, fid -> file_id, v -> version

        # allRows:          the row ids, file ids, and versions of all rows in the target table
        # latestFids:       For all file ids that occur more than once, get the file id and the largest (latest)
        #                   version
        # ridsOfAllFids:    The row ids, file ids, and versions of all file ids present in latestFids; in other words,
        #                   the (row id, file id, version) for every file id that has duplicates
        # ridsOfLatestFids: The row ids, file ids, and versions of all file ids present in latestFids but ONLY the
        #                   latest version rows (so a subset of ridsOfAllFids).
        # Final query:      Get the row ids from ridsOfAllFids but exclude the row ids from ridsOfLatestFids, leaving
        #                   us with the row ids of all non-latest version file ids. These are the rows to soft delete.

        # Note: The EXCEPT DISTINCT SELECT at the end grabs all row ids of rows that AREN'T the latest version.
        # The final subquery here is in case there are multiple rows with the same version.
        query = f"""
        WITH allRows AS (
            SELECT datarepo_row_id AS rid, {target_table}_id AS fid, version AS v
            FROM {sql_table}),
        latestFids AS (
            SELECT DISTINCT fid, MAX(v) AS maxv
            FROM allRows
            GROUP BY fid
            HAVING COUNT(1) > 1),
        ridsOfAllFids AS (
            SELECT t.rid AS rid, f.fid, t.v
            FROM latestFids f
            JOIN allRows t
              ON t.fid = f.fid),
        ridsOfLatestFids AS (
            SELECT t.rid AS rid, f.fid, f.maxv
            FROM latestFids f
            JOIN ridsOfAllFids t
              ON t.fid = f.fid AND t.v = f.maxv)
        SELECT rid
        FROM ridsOfAllFids
        EXCEPT DISTINCT SELECT rid
                        FROM (
                            SELECT MAX(rid) AS rid, fid
                            FROM ridsOfLatestFids
                            GROUP BY fid)
        """

        return self._hit_bigquery(query)

    def get_all_table_names(self) -> set[str]:
        """
        Gets the table names for the target dataset.
        :return: A set of table names.
        """
        query = f"""
        SELECT table_name
        FROM `{self.project}.datarepo_{self.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_type = "VIEW"
        """

        return self._hit_bigquery(query)

    def _process_rows(
        self,
        get_table_names: Callable[[], set[str]],
        get_rids: Callable[[str], set[str]],
        soft_delete: bool,
        issue: str
    ) -> int:
        """
        Perform a check or soft deletion for duplicates or null file references.
        :param get_table_names: A function that returns a set of table names.
        :param get_rids: A function that returns a set of row ids to soft delete.
        :param soft_delete: A flag to indicate whether to just check and print, or to soft delete as well.
        :return: The number of rows to soft delete
        """
        problem_count = 0
        table_names = get_table_names()
        for table_name in table_names:
            rids_to_process = get_rids(table_name)
            if len(rids_to_process) > 0:
                logging.info(f"{table_name} has {len(rids_to_process)} failing rows due to {issue}")
                if soft_delete:
                    local_filename = f"{os.getcwd()}/{table_name}.csv"
                    try:
                        # create and populate file
                        with open(local_filename, mode="w") as wf:
                            populate_row_id_csv(rids_to_process, wf)
                        # do processing
                        self.soft_delete_rows(local_filename, table_name)
                    finally:
                        # delete file
                        os.remove(local_filename)
                problem_count += len(rids_to_process)
        return problem_count

    def _hit_bigquery(self, query: str) -> set[str]:
        """
        Helper function to consistently interact with biqquery while reusing the same client.
        :param query: The SQL query to run.
        :return: A set of whatever the query is asking for (assumes that we're only asking for a single column).
        """
        query_job = self.bigquery_client.query(query)
        return {row[0] for row in query_job}
