"""
Queries the given dataset for a project_id and returns the related
datarepo_row_ids from the project + links table.

This is useful when used in concert with soft deletion to remove a
project from the dataset that should not have been imported. This
_will_ leave garbage behind (i.e., other related entities from the
subgraphs, files, etc.), but will do enough to suppress surfacing the
project in the browser and prevent it from being publicly viewable.

TODO: Automate out the soft deletion process
"""

import argparse
import logging
from typing import Optional

from google.cloud.bigquery import Client

from hca_manage.common import setup_cli_logging_format, DefaultHelpParser
from hca_orchestration.contrib.bigquery import BigQueryService


def run(arguments: Optional[list[str]] = None) -> None:
    setup_cli_logging_format()

    parser = DefaultHelpParser("Finds projects in HCA")

    parser.add_argument("-p", "--hca_project_id", required=True)
    parser.add_argument("-d", "--dataset_name", required=True)
    parser.add_argument("-b", "--bq_project_id", required=True)

    args = parser.parse_args(arguments)
    _query_for_project(args)


def _query_for_project(args: argparse.Namespace) -> None:
    dataset_name = args.dataset_name
    hca_project_id = args.hca_project_id
    bq_project_id = args.bq_project_id

    bq_service = BigQueryService(Client())
    query = f"""
    SELECT * FROM `{dataset_name}.project`
    WHERE project_id = '{hca_project_id}'
    """

    logging.info("Project row IDs = ")
    project_row_ids = bq_service.run_query(query, bq_project_id)
    for row in project_row_ids:
        logging.info(row["datarepo_row_id"])

    query = f"""
    SELECT * FROM `{dataset_name}.links`
    WHERE project_id = '{hca_project_id}'
    """
    links_rows = bq_service.run_query(query, bq_project_id)
    logging.info("")
    logging.info("Links row IDs = ")
    for row in links_rows:
        logging.info(row["datarepo_row_id"])


if __name__ == '__main__':
    run()
