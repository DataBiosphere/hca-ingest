import csv
import os
import logging
from requests import Session
from requests.exceptions import HTTPError
from typing import Set
import urllib.parse

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, storage


# from requests_cache.core import CachedSession


class HcaUtils:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def __init__(self, environment: str, project: str, dataset: str):
        self.environment = environment

        self.project = project

        self.dataset = dataset

        self.filename_template = f"sd-{project}-{dataset}-{{table}}.csv"

        self.bigquery_client = bigquery.Client(project=self.project)

        bucket_projects = {"prod": "mystical-slate-284720",
                           "dev": "broad-dsp-monster-hca-dev"}
        self.bucket_project = bucket_projects[environment]
        self.bucket = f"broad-dsp-monster-hca-{environment}-staging-storage"

        jade_urls = {"prod": "https://jade-terra.datarepo-prod.broadinstitute.org",
                     "dev": "https://jade.datarepo-dev.broadinstitute.org"}
        self.base_url = jade_urls[environment]

        # use application default credentials to seamlessly work across monster devs
        # assumes `gcloud auth application-default login` has been run
        creds, _ = google.auth.default()
        self.gcp_creds = creds

    # bigquery interactions
    def get_all_table_names(self) -> Set[str]:
        """
        Gets the table names for the target dataset.
        :return: A set of table names.
        """
        query = f"""
        SELECT table_name
        FROM `{self.project}.datarepo_{self.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_type = "VIEW"
        """

        return self._hit_bigquery(query)

    def get_file_table_names(self) -> Set[str]:
        """
        Gets the table names for tables that have a `file_id` column.
        :return: A set of table names.
        """
        query = f"""
        WITH fileRefTables AS (SELECT * FROM `{self.project}.datarepo_{self.dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE column_name = "file_id"),
        desiredViews AS (SELECT * FROM `{self.project}.datarepo_{self.dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_type = "VIEW")
        SELECT desiredViews.table_name FROM fileRefTables JOIN desiredViews ON fileRefTables.table_name = desiredViews.table_name
        """

        return self._hit_bigquery(query)

    def get_duplicates(self, target_table: str) -> Set[str]:
        """
        Determines what rows are undesired duplicates. We want to soft delete everything but the latest version for a
        given entity_id.
        :param target_table: The particular table to operate on.
        :return: A set of row ids to soft delete.
        """
        sql_table = f"`{self.project}.datarepo_{self.dataset}.{target_table}`"

        # rid -> row_id, fid -> file_id, v -> version

        # allRows:          the row ids, file ids, and versions of all rows in the target table
        # latestFids:       For all file ids that occur more than once, get the file id and the largest (latest) version
        # ridsOfAllFids:    The row ids, file ids, and versions of all file ids present in latestFids; in other words,
        #                   the (row id, file id, version) for every file id that has duplicates
        # ridsOfLatestFids: The row ids, file ids, and versions of all file ids present in latestFids but ONLY the
        #                   latest version rows (so a subset of ridsOfAllFids).
        # Final query:      Get the row ids from ridsOfAllFids but exclude the row ids from ridsOfLatestFids, leaving us
        #                   with the row ids of all non-latest version file ids. These are the rows to soft delete.

        # Note: The EXCEPT DISTINCT SELECT at the end grabs all row ids of rows that AREN'T the latest version.
        # The final subquery here is in case there are multiple rows with the same version.
        query = f"""
        WITH allRows AS (SELECT datarepo_row_id AS rid, {target_table}_id AS fid, version AS v FROM {sql_table}),
        latestFids AS (SELECT DISTINCT fid, MAX(v) AS maxv FROM allRows GROUP BY fid HAVING COUNT(1) > 1),
        ridsOfAllFids AS (SELECT t.rid AS rid, f.fid, t.v FROM latestFids f JOIN allRows t ON t.fid = f.fid),
        ridsOfLatestFids AS (SELECT t.rid AS rid, f.fid, f.maxv FROM latestFids f JOIN ridsOfAllFids t ON t.fid = f.fid AND t.v = f.maxv)
        SELECT rid FROM ridsOfAllFids EXCEPT DISTINCT SELECT rid FROM (SELECT MAX(rid) AS rid, fid FROM ridsOfLatestFids GROUP BY fid)
        """

        return self._hit_bigquery(query)

    def get_null_filerefs(self, target_table: str) -> Set[str]:
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

    def _hit_bigquery(self, query):
        """
        Helper function to consistently interact with biqquery while reusing the same client.
        :param query: The SQL query to run.
        :return: A set of whatever the query is asking for (assumes that we're only asking for a single column).
        """
        query_job = self.bigquery_client.query(query)
        return {row[0] for row in query_job}

    def _format_filename(self, table: str):
        return self.filename_template.format(table=table)

    # local csv interactions
    @staticmethod
    def populate_row_id_csv(row_ids: Set[str], temp_file):
        """
        Create a csv locally with one column filled with row ids to soft delete.
        :param row_ids: A set of row ids to soft delete.
        :param temp_file: a temporary file to pass in
        :return: The filename of the created csv.
        """
        sd_writer = csv.writer(temp_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        sd_writer.writerows([[rid] for rid in row_ids])

    # gcs (cloud storage) interactions
    def put_csv_in_bucket(self, local_file, target_table: str) -> str:
        """
        Puts a local file into a GCS bucket accessible by Jade.
        :param local_file: The file to upload.
        :param target_table: The table name with which to format the target filename.
        :return: The gs-path of the uploaded file.
        """
        storage_client = storage.Client(project=self.bucket_project, credentials=self.gcp_creds)
        bucket = storage_client.bucket(self.bucket)
        target_filename = self._format_filename(table=target_table)
        blob = bucket.blob(target_filename)
        blob.upload_from_file(local_file)

        filepath = f"gs://{self.bucket}/{target_filename}"
        logging.info(f"Put a soft-delete file here: {filepath}")

        return filepath

    # jade interactions
    def _get_endpoint(self, endpoint: str):
        """
        Get relevant information, such as the HTTP method, the path, and parameters, for Jade API endpoints.
        :param endpoint: The endpoint name to get information for.
        :return: A dictionary containing the endpoint's method, path, and parameters.
        """

        def create_map(paths):
            result_map = {}
            for path, methods in paths.items():
                for method, rest in methods.items():
                    result_map.update(parse_data(method, rest, path))
            return result_map

        def parse_data(method, rest, path):
            return {rest["operationId"]: {"method": method.upper(), "path": path, "params": get_params(rest)}}

        def get_params(rest):
            out = []
            if "parameters" in rest:
                out = [(param["name"], param["in"]) for param in rest["parameters"]]
            return out

        url = f"{self.base_url}/v2/api-docs"
        response = Session().get(url=url)
        paths = response.json()["paths"]

        endpoint_map = create_map(paths)

        try:
            return endpoint_map[endpoint]
        except KeyError:
            raise KeyError(f"Endpoint named {endpoint} not found!")

    def _hit_jade(self, endpoint, body=None, params=None, query=None):
        """
        A generic function for hitting the Jade API.
        :param endpoint: The endpoint to interact with.
        :param handle_ok: A function that handles the response from the server if the response is a 2xx code.
        :param body: An optional body for the API request if needed.
        :param params: Optional endpoint parameters (not query filters) for the API request if needed.
        :param query: Optional query parameters for the API request if needed.
        :return: The result of running handle_ok on the server's response, or an HTTPError.
        """
        ep_info = self._get_endpoint(endpoint)
        url = f"{self.base_url}{ep_info['path']}"

        if params:
            url = url.format(**params)
        if query:
            url = f"{url}/?{urllib.parse.urlencode(query)}"

        response = AuthorizedSession(self.gcp_creds).request(method=ep_info["method"], url=url, json=body)

        if response.ok:
            return response
        else:
            raise HTTPError(f"Bad response, got code of: {response.status_code} with response body {response.text}")

    def get_dataset_id(self) -> str:
        """
        Get the dataset ID of the provided dataset name.
        :return: The dataset id.
        """

        response = self._hit_jade("enumerateDatasets", query={"filter": self.dataset})
        return response.json()["items"][0]["id"]

    def submit_soft_delete(self, target_table: str, target_path: str) -> str:
        """
        Submit a soft delete request.
        :param target_table: The table to apply soft deletion to.
        :param target_path: The gs-path of the csv that contains the row ids to soft delete.
        :return: The job id of the soft delete job.
        """
        dataset_id = self.get_dataset_id()

        body = {
            "deleteType": "soft",
            "specType": "gcsFile",
            "tables": [
                {
                    "gcsFileSpec": {
                        "fileType": "csv",
                        "path": target_path
                    },
                    "tableName": target_table
                }
            ]
        }

        response = self._hit_jade("applyDatasetDataDeletion", body=body, params={"id": dataset_id})

        return response.json()["id"]

    # dataset-level checking and soft deleting
    def process_duplicates(self, soft_delete: bool = False):
        """
        Check and print the number of duplicates for each table in the dataset.
        :return:
        """
        return self._process_rows(self.get_all_table_names, self.get_duplicates, soft_delete=soft_delete,
                                  issue="duplicate rows")

    def process_null_file_refs(self, soft_delete: bool = False):
        """
        Check/remove and print the number of null file references for each table in the dataset that has a `file_id`
        column.
        :return:
        """
        return self._process_rows(self.get_file_table_names, self.get_null_filerefs, soft_delete=soft_delete,
                                  issue="null file refs")

    def check_for_all(self):
        """
        Check and print the number of duplicates and null file references in all tables in the dataset.
        :return:
        """
        logging.info("Processing...")
        duplicate_count = self.process_duplicates()
        null_file_ref_count = self.process_null_file_refs()
        logging.info("Finished.")
        return duplicate_count + null_file_ref_count

    def remove_all(self):
        """
        Check and print the number of duplicates and null file references for each table in the dataset, then soft
        delete the problematic rows.
        :return:
        """
        logging.info("Processing, deleting as we find anything...")
        duplicate_count = self.process_duplicates(soft_delete=True)
        null_file_ref_count = self.process_null_file_refs(soft_delete=True)
        logging.info("Finished.")
        return duplicate_count + null_file_ref_count

    def _process_rows(self, get_table_names, get_rids, soft_delete: bool, issue: str) -> int:
        """
        Perform a check or soft deletion for duplicates or null file references.
        :param get_table_names: A function that returns a set of table names.
        :param get_rids: A function that returns a set of row ids to soft delete.
        :param soft_delete: A flag to indicate whether to just check and print, or to soft delete as well.
        :return:
        """
        problem_count = 0
        table_names = get_table_names()
        for table_name in table_names:
            rids_to_process = get_rids(table_name)
            if len(rids_to_process) > 0:
                logging.info(f"{table_name} has {len(rids_to_process)} rows to soft delete due to {issue}")
                if soft_delete:
                    local_filename = f"{os.getcwd()}/{table_name}.csv"
                    try:
                        # create and populate file
                        with open(local_filename, mode="w") as wf:
                            self.populate_row_id_csv(rids_to_process, wf)
                        # do processing
                        with open(local_filename, mode="rb") as rf:
                            remote_file_path = self.put_csv_in_bucket(local_file=rf, target_table=table_name)
                            job_id = self.submit_soft_delete(table_name, remote_file_path)
                            logging.info(f"Soft delete job for table {table_name} running, job id of: {job_id}")
                    finally:
                        # delete file
                        os.remove(local_filename)
                problem_count += len(rids_to_process)
        return problem_count
