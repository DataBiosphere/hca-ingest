import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

from google.cloud import bigquery

from hca_manage.common import populate_row_id_csv
from hca_manage.soft_delete import SoftDeleteManager


# this hacky nonsense is because mypy currently can't reckon with abstract dataclasses
# see here for explanation/updates: https://github.com/python/mypy/issues/5374
@dataclass
class _BQRowDataclass:
    dataset: str
    project: str
    soft_delete_manager: SoftDeleteManager


class BQRowManager(_BQRowDataclass, ABC):
    @property
    def bigquery_client(self) -> bigquery.client.Client:
        return bigquery.Client(project=self.project)

    @abstractmethod
    def get_rows(self, target_table: str) -> set[str]:
        pass

    @abstractmethod
    def check_or_delete_rows(self, soft_delete: bool = False) -> int:
        pass

    def _hit_bigquery(self, query: str) -> set[str]:
        """
        Helper function to consistently interact with biqquery while reusing the same client.
        :param query: The SQL query to run.
        :return: A set of whatever the query is asking for (assumes that we're only asking for a single column).
        """
        query_job = self.bigquery_client.query(query)
        return {row[0] for row in query_job}

    def _check_or_delete_rows(
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
                        self.soft_delete_manager.soft_delete_rows(local_filename, table_name)
                    finally:
                        # delete file
                        os.remove(local_filename)
                problem_count += len(rids_to_process)
        return problem_count


@dataclass
class CountsManager(BQRowManager):
    entity_type: str

    def get_rows(self, target_table: str) -> set[str]:
        query = f"""
        SELECT COUNT(*) FROM `{self.project}.{self.dataset}.{self.entity_type}`
        """
        cnt = int(self._hit_bigquery(query).pop())
        if not cnt:
            return {"no rows"}

        return set()

    def check_or_delete_rows(self, soft_delete: bool = False) -> int:
        if soft_delete:
            raise NotImplementedError("Soft deleting rows in this context is unsupported")

        def tables() -> set[str]:
            return {self.entity_type}

        return self._check_or_delete_rows(tables, self.get_rows, soft_delete=False,
                                          issue=f"Found empty {self.entity_type} table")


class DanglingFileRefManager(BQRowManager):
    def get_rows(self, target_table: str) -> set[str]:
        query = f"""
        SELECT links.links_id
        FROM `{self.project}.{self.dataset}.{target_table}` links
                 LEFT JOIN
             `{self.project}.{self.dataset}.project` projects
             ON
                 {target_table}.project_id = projects.project_id
        WHERE projects.project_id IS NULL;
        """
        return self._hit_bigquery(query)

    def check_or_delete_rows(self, soft_delete: bool = False) -> int:
        """
        Check for any entities with project_id values that do not have a corresponding entry in the projects
        table
        :return: Number of rows with dangling project refs
        """
        if soft_delete:
            raise NotImplementedError("Soft deleting rows with dangling project refs is unsupported")

        def links_table() -> set[str]:
            return {'links'}

        return self._check_or_delete_rows(links_table, self.get_rows, soft_delete=soft_delete,
                                          issue="found rows with dangling project refs")


class DuplicatesManager(BQRowManager):
    def get_rows(self, target_table: str) -> set[str]:
        """
        Determines what rows are undesired duplicates. We want to soft delete everything but the latest version for a
        given entity_id.
        :param target_table: The particular table to operate on.
        :return: A set of row ids to soft delete.
        """
        sql_table = f"`{self.project}.{self.dataset}.{target_table}`"

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

    def check_or_delete_rows(self, soft_delete: bool = False) -> int:
        """
        Check and print the number of duplicates for each table in the dataset.
        :return: Number of duplicate rows to soft delete
        """
        return self._check_or_delete_rows(self.get_all_table_names,
                                          self.get_rows,
                                          soft_delete=soft_delete,
                                          issue="duplicate rows")

    def get_all_table_names(self) -> set[str]:
        """
        Gets the table names for the target dataset.
        :return: A set of table names.
        """
        query = f"""
        SELECT table_name
        FROM `{self.project}.{self.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_type = "VIEW"
        """

        return self._hit_bigquery(query)


class NullFileRefManager(BQRowManager):
    def get_rows(self, target_table: str) -> set[str]:
        """
        Determines what rows have null values in the file_id column. We want to soft delete those.
        :param target_table: The particular table to operate on.
        :return: A set of row ids to soft delete.
        """

        # TODO we are allowing null file_ids if there is a `drs_uri` field in the descriptor (these are
        # "bring your own" DRS file references that are hosted outside of TDR). We should be smarter about parsing
        # the descriptor and ensuring a sane value rather than the fuzzy matching we're doing here
        query = f"""
        SELECT datarepo_row_id
        FROM `{self.project}.{self.dataset}.{target_table}` WHERE file_id IS NULL AND descriptor NOT LIKE '%"drs_uri":%'
        """

        return self._hit_bigquery(query)

    def check_or_delete_rows(self, soft_delete: bool = False) -> int:
        """
        Check/remove and print the number of null file references for each table in the dataset that has a `file_id`
        column.
        :return: Number of rows with null file refs to soft delete
        """
        return self._check_or_delete_rows(self.get_file_table_names, self.get_rows, soft_delete=soft_delete,
                                          issue="null file refs")

    def get_file_table_names(self) -> set[str]:
        """
        Gets the table names for tables that have a `file_id` column.
        :return: A set of table names.
        """
        query = f"""
        WITH fileRefTables AS (
            SELECT *
            FROM `{self.project}.{self.dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE column_name = "file_id"),
        desiredViews AS (
            SELECT * FROM `{self.project}.{self.dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = "VIEW")
        SELECT desiredViews.table_name
        FROM fileRefTables
        JOIN desiredViews
          ON fileRefTables.table_name = desiredViews.table_name
        """

        return self._hit_bigquery(query)
