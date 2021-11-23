"""
Verifies that all nodes in the subgraphs of a dataset or snapshot are loaded
"""
import argparse
from typing import Optional

from google.cloud.bigquery import Client, ArrayQueryParameter
from google.cloud.bigquery.table import RowIterator

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_manage.common import data_repo_host, get_api_client
from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.models.entities import MetadataEntity, build_subgraph_from_links_row
from hca_orchestration.support.subgraphs import build_subgraph_nodes
from hca_orchestration.support.typing import MetadataType


def verify_all_subgraphs_in_dataset(links_rows: RowIterator, bq_project: str, dataset: str, client: Client) -> None:
    links = [
        build_subgraph_from_links_row(row) for row in links_rows
    ]
    nodes = build_subgraph_nodes(links)

    for metadata_type, expected_entities in nodes.items():
        print(f"Getting loaded IDs [entity_type={metadata_type}]")
        verify_entities_loaded(metadata_type, expected_entities, bq_project, dataset, client)


def verify_entities_loaded(entity_type: MetadataType, expected_entities: list[MetadataEntity], bq_project: str,
                           dataset: str, bigquery_service: BigQueryService) -> None:
    fetch_entities_query = f"""
        SELECT {entity_type}_id
        FROM `{bq_project}.{dataset}.{entity_type}` WHERE {entity_type}_id IN
        UNNEST(@entity_ids)
    """

    expected_ids = {entity.entity_id for entity in expected_entities}
    query_params = [
        ArrayQueryParameter("entity_ids", "STRING", expected_ids)
    ]

    loaded_ids = {row[f'{entity_type}_id'] for row in bigquery_service.run_query(
        fetch_entities_query, bigquery_project=bq_project, location='US', query_params=query_params
    )}

    set_diff = expected_ids - loaded_ids
    assert len(set_diff) == 0, f"Not all expected IDs found [diff = {set_diff}]"


def run(bq_project: str, dataset: str, snapshot: bool, project_id: str) -> None:
    bigquery_service = BigQueryService(Client(project=bq_project))
    if not snapshot:
        dataset = f"datarepo_{dataset}"

    print(f"Querying bq... [project={bq_project}, dataset={dataset}]")
    query = f"""
    SELECT * FROM `{bq_project}.{dataset}.links`
    """

    if project_id:
        query = query + f"""  WHERE project_id = '{project_id}'"""

    links_rows = [row for row in bigquery_service.run_query(query, bq_project, 'US')]
    verify_all_subgraphs_in_dataset(links_rows, bq_project, dataset, bigquery_service)


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-b", "--bq-project", required=True)
    argparser.add_argument("-d", "--dataset", required=True)
    argparser.add_argument("-s", "--snapshot", action="store_true")
    argparser.add_argument("-p", "--project_id")
    args = argparser.parse_args()

    run(args.bq_project, args.dataset, args.snapshot, args.project_id)
