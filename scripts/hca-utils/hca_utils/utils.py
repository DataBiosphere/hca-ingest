from typing import Set

from google.cloud import bigquery


class HcaUtils:
    def __init__(self, environment: str, project: str, dataset: str):
        self.environment = environment

        if environment == "dev":
            self.project = "broad-jade-dev-data"
        else:
            self.project = project

        self.dataset = dataset

        self.bigquery_client = bigquery.Client(project=self.project)

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

    def _hit_bigquery(self, query):
        """
        Helper function to consistently interact with biqquery while reusing the same client.
        :param query: The SQL query to run.
        :return: A set of whatever the query is asking for (assumes that we're only asking for a single column).
        """
        query_job = self.bigquery_client.query(query)
        return {row[0] for row in query_job}
