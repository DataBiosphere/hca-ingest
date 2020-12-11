import csv
from functools import lru_cache
import os
from requests.exceptions import HTTPError
from typing import Set
import urllib.parse

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, storage
from requests_cache.core import CachedSession


class HcaUtils:
    def __init__(self, environment: str, project: str, dataset: str):
        self.environment = environment

        if environment == "dev":
            self.project = "broad-jade-dev-data"
        else:
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

        # this is always prod for bigquery, storage, etc.
        gcp_creds, _ = google.auth.load_credentials_from_file("path/to/prod/credentials.json")
        self.gcp_creds = gcp_creds

        # this depends on the Jade env to interact with
        creds_path = {"prod": "path/to/prod/credentials.json",
                      "dev": "path/to/dev/credentials.json"}
        jade_creds, _ = google.auth.load_credentials_from_file(creds_path[environment])
        self.jade_creds = jade_creds

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

    # local csv interactions
    def create_csv(self, row_ids: Set[str], target_table: str) -> str:
        """
        Create a csv locally with one column filled with row ids to soft delete.
        :param row_ids: A set of row ids to soft delete.
        :param target_table: The table that the row ids belong to.
        :return: The filename of the created csv.
        """
        filename = self.filename_template.format(table=target_table)
        with open(filename, mode="w") as soft_delete_file:
            sd_writer = csv.writer(soft_delete_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

            for rid in row_ids:
                sd_writer.writerow([rid])

        return filename

    @staticmethod
    def delete_csv(filename: str):
        """
        Remove the csv after it has been uploaded so that there isn't a deluge of csv files in the local directory.
        :param filename: The filename of the file to delete.
        :return:
        """
        os.remove(filename)

    # gcs (cloud storage) interactions
    def put_csv_in_bucket(self, filename: str) -> str:
        """
        Puts a local file into a GCS bucket accessible by Jade.
        :param filename: The filename of the file to upload.
        :return: The gs-path of the uploaded file.
        """
        storage_client = storage.Client(project=self.bucket_project, credentials=self.gcp_creds)
        bucket = storage_client.bucket(self.bucket)
        blob = bucket.blob(filename)
        blob.upload_from_filename(filename)

        filepath = f"gs://{self.bucket}/{filename}"
        print(f"Put a soft-delete file here: {filepath}")

        return filepath

    # jade interactions
    @staticmethod
    @lru_cache()
    def _get_endpoint(endpoint: str):
        """
        Get relevant information, such as the HTTP method, the path, and parameters, for Jade API endpoints.
        :param endpoint: The endpoint name to get information for.
        :return: A dictionary containing the endpoint's method, path, and parameters.
        """
        url = "https://jade-terra.datarepo-prod.broadinstitute.org/v2/api-docs"
        response = CachedSession().get(url=url)
        paths = response.json()["paths"]

        my_map = {}

        for path, methods in paths.items():
            for method, rest in methods.items():
                if "parameters" in rest:
                    params = [(param["name"], param["in"]) for param in rest["parameters"]]
                else:
                    params = []
                my_map.update({rest["operationId"]: {"method": method.upper(), "path": path, "params": params}})
        if endpoint in my_map:
            return my_map[endpoint]
        else:
            raise KeyError(f"Endpoint named {endpoint} not found!")

    def _hit_jade(self, endpoint, handle_ok, body=None, params=None, query=None):
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

        response = AuthorizedSession(self.jade_creds).request(method=ep_info["method"], url=url, json=body)

        if response.ok:
            return handle_ok(response)
        else:
            raise HTTPError(f"Bad response, got code of: {response.status_code} with response body {response.text}")

    def get_dataset_id(self) -> str:
        """
        Get the dataset ID of the provided dataset name.
        :return: The dataset id.
        """

        def handle_response(response):
            return response.json()["items"][0]["id"]

        dataset_id = self._hit_jade("enumerateDatasets", handle_response, query={"filter": self.dataset})
        return dataset_id

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

        def handle_response(response):
            return response.json()["id"]

        job_id = self._hit_jade("applyDatasetDataDeletion", handle_response, body=body, params={"id": dataset_id})

        return job_id

