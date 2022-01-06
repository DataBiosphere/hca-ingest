"""
Verifies that all nodes in the subgraphs of a dataset or snapshot are loaded
"""
import argparse
import csv
import logging

from data_repo_client import SnapshotModel
from google.cloud.bigquery import ArrayQueryParameter, Client
from google.cloud.bigquery.table import RowIterator

from hca_manage.common import (
    data_repo_host,
    get_api_client,
    setup_cli_logging_format,
)
from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.contrib.data_repo.data_repo_service import (
    DataRepoService,
)
from hca_orchestration.models.entities import (
    MetadataEntity,
    build_subgraph_from_links_row,
)
from hca_orchestration.support.subgraphs import build_subgraph_nodes
from hca_orchestration.support.typing import MetadataType


class SubgraphValidationException(Exception):
    pass


def verify_all_subgraphs_in_dataset(links_rows: RowIterator, bq_project: str,
                                    dataset: str, bigquery_service: BigQueryService) -> None:
    links = [
        build_subgraph_from_links_row(row) for row in links_rows
    ]
    nodes = build_subgraph_nodes(links)

    for metadata_type, expected_entities in nodes.items():
        logging.debug(f"Getting loaded IDs [entity_type={metadata_type}]")
        verify_entities_loaded(metadata_type, expected_entities, bq_project, dataset, bigquery_service)


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


def run(bq_project: str, dataset: str, snapshot: bool, project_id: str, project_only: bool = True) -> None:
    bigquery_service = BigQueryService(Client(project=bq_project))
    if not snapshot:
        dataset = f"datarepo_{dataset}"

    logging.info(f"Querying bq... [project={bq_project}, dataset={dataset}, hca_project_id={project_id}]")
    query = f"""
    SELECT * FROM `{bq_project}.{dataset}.links`
    """

    if project_id and not project_only:
        query = query + f"""  WHERE project_id = '{project_id}'"""

    links_rows = [row for row in bigquery_service.run_query(query, bq_project, 'US')]
    assert len(links_rows) > 0, f"Should have links rows for project_id {project_id}"

    if project_only:
        logging.info(f"Verifying dataset contains data for single project only [project_id={project_id}]")
        for row in links_rows:
            assert row[
                "project_id"] == project_id, f"Dataset should only contain links rows for single project [project_id={project_id}]"

    verify_all_subgraphs_in_dataset(links_rows, bq_project, dataset, bigquery_service)

    logging.info(
        f"✅ Subgraphs verified [project_id={project_id}, dataset={dataset}, bq_project={bq_project}, num_links={len(links_rows)}]")


def verify_snapshot_for_project(source_hca_project_id: str, dataset_qualifier: str) -> SnapshotModel:
    host = data_repo_host["real_prod"]
    data_repo_client = get_api_client(host=host)
    data_repo_service = DataRepoService(data_repo_client=data_repo_client)
    sanitized_hca_project_name = source_hca_project_id.replace('-', '')
    source_hca_dataset_prefix = f"hca_prod_{sanitized_hca_project_name}"

    tdr_dataset = data_repo_service.find_dataset(source_hca_dataset_prefix, qualifier=dataset_qualifier)
    snapshots = data_repo_client.enumerate_snapshots(filter=tdr_dataset.dataset_name)

    if not snapshots.items:
        logging.info(f"❌ No snapshot found [hca_project_id={source_hca_project_id}")
        raise SubgraphValidationException()

    if len(snapshots.items) > 1:
        logging.info(f"❌ Found more than one snapshot [hca_project_id={source_hca_project_id}]")
        for snapshot in snapshots.items:
            logging.info(f"\t snapshot name = {snapshot.name}")

        raise SubgraphValidationException()

    snapshot = snapshots.items[0]
    full_snapshot_info: SnapshotModel = data_repo_client.retrieve_snapshot(id=snapshot.id)

    run(
        bq_project=full_snapshot_info.data_project,
        dataset=full_snapshot_info.name,
        snapshot=True,
        project_id=source_hca_project_id,
        project_only=True
    )

    return full_snapshot_info


def verify_snapshots(project_list_path):
    with open(project_list_path) as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                verify_snapshot_for_project(row[0], row[1])
            except SubgraphValidationException as e:
                logging.error(f"Could not validate project_id {row[0]}")


if __name__ == '__main__':
    setup_cli_logging_format()
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-c", "--project_list")

    args = argparser.parse_args()
    verify_snapshots(args.project_list)
    #find_snapshot_for_project(args.project_id, args.dataset_qualifier)

    #
    # argparser.add_argument("-b", "--bq-project", required=True)
    # argparser.add_argument("-d", "--dataset", required=True)
    # argparser.add_argument("-s", "--snapshot", action="store_true")
    # argparser.add_argument("-p", "--project_id")
    # args = argparser.parse_args()
    #
    # run(args.bq_project, args.dataset, args.snapshot, args.project_id)
