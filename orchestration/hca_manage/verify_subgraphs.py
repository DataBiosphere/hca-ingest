"""
Verifies that all nodes in the subgraphs of a dataset or snapshot are loaded
"""
import argparse

from google.cloud.bigquery import Client, ArrayQueryParameter, QueryJobConfig, Row

from hca_orchestration.models.entities import MetadataEntity, build_subgraph_from_links_row
from hca_orchestration.support.subgraphs import build_subgraph_nodes
from hca_orchestration.support.typing import MetadataType


def verify_all_subgraphs_in_dataset(links_rows: list[Row], bq_project: str, dataset: str, client: Client) -> None:
    links = [
        build_subgraph_from_links_row(row) for row in links_rows
    ]
    nodes = build_subgraph_nodes(links)

    for metadata_type, expected_entities in nodes.items():
        print(f"Getting loaded IDs [entity_type={metadata_type}]")
        verify_entities_loaded(metadata_type, expected_entities, bq_project, dataset, client)


def verify_entities_loaded(entity_type: MetadataType, expected_entities: list[MetadataEntity], bq_project: str,
                           dataset: str, client: Client) -> None:
    fetch_entities_query = f"""
        SELECT {entity_type}_id
        FROM `{bq_project}.{dataset}.{entity_type}` WHERE {entity_type}_id IN
        UNNEST(@entity_ids)
    """

    expected_ids = {entity.entity_id for entity in expected_entities}
    query_params = [
        ArrayQueryParameter("entity_ids", "STRING", expected_ids)
    ]
    job_config = QueryJobConfig()
    job_config.query_parameters = query_params

    loaded_ids = {row[f'{entity_type}_id'] for row in client.query(
        fetch_entities_query, project=bq_project, job_config=job_config
    )}

    set_diff = expected_ids - loaded_ids
    assert len(set_diff) == 0, f"Not all expected IDs found [diff = {set_diff}]"


def run(bq_project: str, dataset: str, snapshot: bool) -> None:
    client = Client(project=bq_project)
    if not snapshot:
        dataset = f"datarepo_{dataset}"

    print(f"Querying bq... [project={bq_project}, dataset={dataset}]")

    query = f"""
    SELECT * FROM `{bq_project}.{dataset}.links`
    """
    links_rows = [row for row in client.query(query).result()]
    verify_all_subgraphs_in_dataset(links_rows, bq_project, dataset, client)


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-b", "--bq-project", required=True)
    argparser.add_argument("-d", "--dataset", required=True)
    argparser.add_argument("-s", "--snapshot", action="store_true")
    args = argparser.parse_args()

    run(args.bq_project, args.dataset, args.snapshot)
