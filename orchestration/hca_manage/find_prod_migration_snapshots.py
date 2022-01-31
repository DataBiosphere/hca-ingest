"""
Given a source snapshot, this script will scan real prod for corresponding single-project snapshots for each
HCA project in the source
"""

import argparse

from google.cloud.bigquery import Client

from hca_manage.common import data_repo_host, get_api_client
from hca_orchestration.contrib.bigquery import BigQueryService

CATALOGS = {
    "dcp1": {
        "dataset_name": "hca_prod_20201118_dcp1___20201209",
        "bq_project": "broad-datarepo-terra-prod-hca2"
    },
    "dcp2": {
        "dataset_name": "hca_prod_20201120_dcp2___20211213_dcp12",
        "bq_project": "tdr-fp-fea71bda"
    },
}


def run(catalog: str):
    host = data_repo_host["real_prod"]
    client = get_api_client(host=host)

    if catalog not in CATALOGS:
        raise Exception(f"Catalog {catalog} not found")

    catalog_info = CATALOGS[catalog]
    bq_project = catalog_info['bq_project']
    dataset_name = catalog_info['dataset_name']

    bq_service = BigQueryService(Client())
    query = f"""
    SELECT project_id FROM `{bq_project}.{dataset_name}.project` ORDER BY project_id ASC
    """
    project_row_ids = bq_service.run_query(query, bq_project, "US")

    expected_project_ids = set()
    for row in project_row_ids:
        expected_project_ids.add(row['project_id'].replace('-', ''))

    published_snapshots = {}
    for expected_project_id in expected_project_ids:

        result = client.enumerate_snapshots(filter=f"hca_prod_{expected_project_id}")
        found_expected = False
        for item in result.items:
            name = item.name
            if f'_{catalog}_' in name:
                published_snapshots[expected_project_id] = name
                print(f"✅ - Found snapshot for {expected_project_id} [{name}]")
                found_expected = True
                break

        if not found_expected:
            print(f"❌ - Did not find snapshot for {expected_project_id}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", '--catalog', required=True)
    args = parser.parse_args()
    run(args.catalog)
