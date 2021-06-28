import argparse
from multiprocessing import Pool
from dataclasses import dataclass
from google.cloud.bigquery.client import Client
from dagster_utils.contrib.google import authorized_session

from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import FileMetadataTypes
from hca_orchestration.solids.load_hca.non_file_metadata.load_non_file_metadata import NonFileMetadataTypes


@dataclass
class DbDiffWorkItem:
    dagster_dataset_name: str
    argo_dataset_name: str
    table_name: str
    except_fields: list[str]


def diff_tables(work_item: DbDiffWorkItem) -> None:
    bigquery_client = Client(_http=authorized_session())

    table_name = work_item.table_name
    primary_key = f"{work_item.table_name}_id"
    except_clause = f",".join(work_item.except_fields)
    query = f"""
            WITH argo AS (
              SELECT * EXCEPT ({except_clause}) FROM `broad-jade-dev-data.datarepo_{work_item.argo_dataset_name}.{table_name}`
            ),
            dagster AS (
             SELECT * EXCEPT ({except_clause}) FROM `broad-jade-dev-data.datarepo_{work_item.dagster_dataset_name}.{table_name}`
             )
            SELECT * FROM argo FULL OUTER JOIN dagster
            ON argo.{primary_key} = dagster.{primary_key}
            WHERE to_json_string(argo) != to_json_string(dagster)
            """

    result = bigquery_client.query(query, None).result()
    rows = [row for row in result]
    if rows:
        print(f"❌ diff in {table_name}")
        for row in rows:
            print(row)
    else:
        print(f"✅ no diff in {table_name}")


def diff_dbs(args: argparse.Namespace) -> None:
    diff_file_data(args.dagster_dataset_name, args.argo_dataset_name)
    diff_non_file_data(args.dagster_dataset_name, args.argo_dataset_name)


def diff_file_data(dagster_dataset_name: str, argo_dataset_name: str) -> None:
    work_items = [
        DbDiffWorkItem(dagster_dataset_name, argo_dataset_name, data_type.value, ["datarepo_row_id", "file_id"])
        for data_type in FileMetadataTypes
    ]

    with Pool(4) as p:
        p.map(diff_tables, work_items)


def diff_non_file_data(dagster_dataset_name: str, argo_dataset_name: str) -> None:
    work_items = [
        DbDiffWorkItem(dagster_dataset_name, argo_dataset_name, data_type.value, ["datarepo_row_id"])
        for data_type in NonFileMetadataTypes
    ]

    with Pool(4) as p:
        p.map(diff_tables, work_items)
