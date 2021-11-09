from google.cloud.bigquery import QueryJobConfig, Client


def assert_data_loaded(file_type: str, dataset_name: str, bq_project: str, tdr_bigquery_client: Client):
    files_loaded = _query_files_loaded(
        file_type,
        dataset_name,
        bq_project,
        tdr_bigquery_client
    )
    assert len(files_loaded) > 0, f"Should have loaded {file_type} data files"


def assert_metadata_loaded(metadata_type: str, dataset_name: str, bq_project: str, tdr_bigquery_client: Client):
    data_loaded = _query_metadata_table(
        metadata_type,
        dataset_name,
        bq_project,
        tdr_bigquery_client
    )

    assert len(data_loaded) > 0, f"Should have loaded {metadata_type} rows"


def _query_metadata_table(metadata_type: str, dataset_name: str, bq_project: str, client: Client):
    query = f"""
    SELECT * FROM `datarepo_{dataset_name}.{metadata_type}`
    """
    return _exec_query(query, client, bq_project)


def _query_files_loaded(file_type: str, dataset_name: str, bq_project: str, client: Client):
    query = f"""
    SELECT * FROM `datarepo_{dataset_name}.{file_type}` f
    INNER JOIN `datarepo_{dataset_name}.datarepo_load_history` dlh
    ON dlh.file_id = f.file_id
    """
    return _exec_query(query, client, bq_project)


def _exec_query(query, client, bq_project):
    job_config = QueryJobConfig()
    job_config.use_legacy_sql = False

    query_job = client.query(
        query,
        job_config,
        location="US",
        project=bq_project
    )
    result = query_job.result()
    return [row for row in result]
