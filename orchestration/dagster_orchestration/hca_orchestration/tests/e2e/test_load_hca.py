import pytest
from dagster import execute_pipeline
from google.cloud.bigquery.client import Client, QueryJobConfig

from hca_orchestration.pipelines import load_hca


@pytest.mark.skip
@pytest.mark.e2e
def test_load_hca(load_hca_run_config, dataset_name, tdr_bigquery_client):
    execute_pipeline(
        load_hca,
        mode="local",
        run_config=load_hca_run_config
    )

    sequence_file_rows = _query_metadata_table(
        "sequence_file",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(sequence_file_rows) > 0, "Should have sequence_file rows"

    cell_suspension_rows = _query_metadata_table(
        "cell_suspension",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(cell_suspension_rows) > 0, "Should have cell_suspension rows"

    collection_protocol_rows = _query_metadata_table(
        "collection_protocol",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(collection_protocol_rows) > 0, "Should have collection_protocol rows"

    dissociation_protocol_rows = _query_metadata_table(
        "dissociation_protocol",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(dissociation_protocol_rows) > 0, "Should have dissociation_protocol rows"

    donor_organism_rows = _query_metadata_table(
        "donor_organism",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(donor_organism_rows) > 0, "Should have donor_organism rows"

    library_preparation_protocol_rows = _query_metadata_table(
        "library_preparation_protocol",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(library_preparation_protocol_rows) > 0, "Should have library_preparation_protocol rows"

    process_rows = _query_metadata_table(
        "process",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(process_rows) > 0, "Should have process rows"

    project_rows = _query_metadata_table(
        "process",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(project_rows) > 0, "Should have project rows"

    sequencing_protocol_rows = _query_metadata_table(
        "sequencing_protocol",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(sequencing_protocol_rows) > 0, "Should have sequencing_protocol rows"

    specimen_from_organism_rows = _query_metadata_table(
        "specimen_from_organism",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(specimen_from_organism_rows) > 0, "Should have specimen_from_organism rows"

    links_rows = _query_metadata_table(
        "links",
        dataset_name,
        tdr_bigquery_client
    )
    assert len(links_rows) > 0, "Should have links rows"


def _query_metadata_table(metadata_type: str, dataset_name: str, client: Client):
    query = f"""
    SELECT * FROM `datarepo_{dataset_name}.{metadata_type}`
    """
    job_config = QueryJobConfig()
    job_config.use_legacy_sql = False

    query_job = client.query(
        query,
        job_config,
        location="US",
        project="broad-jade-dev-data"
    )
    result = query_job.result()
    return [row for row in result]
