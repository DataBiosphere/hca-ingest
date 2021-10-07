"""
Verifies that all nodes in the subgraphs of a dataset are loaded
"""
import argparse
import json
from collections import defaultdict

from google.cloud.bigquery import Client, ArrayQueryParameter, QueryJobConfig

from hca_orchestration.models.entities import MetadataEntity
from hca_orchestration.support.typing import MetadataType


def hydrate_subgraph(links_rows: list, bq_project: str, dataset: str, client: Client):
    nodes = defaultdict(list)
    subgraphs = []
    for row in links_rows:
        subgraphs.append(json.loads(row["content"])["links"])
        nodes["links"].append(MetadataEntity(MetadataType("link"), row["links_id"]))

    print(f"Hydrating subgraphs [count={len(subgraphs)}]")
    for subgraph in subgraphs:
        for link in subgraph:
            link_type = link["link_type"]
            if link_type == 'process_link':
                process = MetadataEntity(link["process_type"], link["process_id"])
                nodes[process.entity_type].append(process)

                for input_link in link["inputs"]:
                    input_entity = MetadataEntity(input_link["input_type"], input_link["input_id"])
                    nodes[input_entity.entity_type].append(input_entity)

                for output_link in link["outputs"]:
                    output_entity = MetadataEntity(output_link["output_type"], output_link["output_id"])
                    nodes[output_entity.entity_type].append(output_entity)

                for protocol_link in link["protocols"]:
                    protocol_entity = MetadataEntity(protocol_link["protocol_type"], protocol_link["protocol_id"])
                    nodes[protocol_entity.entity_type].append(protocol_entity)

            elif link_type == 'supplementary_file_link':
                entity = MetadataEntity(link["entity"]["entity_type"], link["entity"]["entity_id"])
                nodes[entity.entity_type].append(entity)

                for file_link in link['files']:
                    file_entity = MetadataEntity(file_link["file_type"], file_link["file_id"])
                    nodes[file_entity.entity_type].append(file_entity)
            else:
                raise Exception(f"Unknown link type {link_type} encountered")

    for metadata_type, expected_entities in nodes.items():
        print(f"Getting loaded IDs [entity_type={metadata_type}]")
        verify_entities_loaded(metadata_type, expected_entities, bq_project, dataset, client)


def verify_entities_loaded(entity_type: MetadataType, expected_entities: list[MetadataEntity], bq_project: str,
                           dataset: str, client: Client):
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


def run(bq_project: str, dataset: str, snapshot: bool):
    client = Client(project=bq_project)
    if not snapshot:
        dataset = f"datarepo_{dataset}"

    print(f"Querying bq... [project={bq_project}, dataset={dataset}]")
    query = f"""
    SELECT * FROM `{bq_project}.{dataset}.links`
    """

    links_rows = [row for row in client.query(query).result()]
    hydrate_subgraph(links_rows, bq_project, dataset, client)


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-b", "--bq-project", required=True)
    argparser.add_argument("-d", "--dataset", required=True)
    argparser.add_argument("-s", "--snapshot", action="store_true")
    args = argparser.parse_args()

    run(args.bq_project, args.dataset, args.snapshot)
