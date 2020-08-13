import google.auth
from google.cloud import bigquery
import json
import os

# take contextual information as arguments
project_id = os.environ["PROJECT_ID"]
dataset_name = os.environ["DATASET_NAME"]
load_tag = os.environ["LOAD_TAG"]

# Set up BigQuery client
credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])
bqclient = bigquery.Client(credentials=credentials, project=project_id,)


# log a checksum error for the given source and target path
def log_checksum_error(source_name: str, target_path: str):
    error_log = {
        "errorType": "ChecksumError",
        "filePath": source_name,
        "fileName": source_name.split("/")[-1],
        "message": f"New crc32c checksum does not match the original for the file ingested to {target_path}."
    }
    with open('../../logs/errors.log', 'a+') as log_file:
        log_file.write(json.dumps(error_log) + "\n")


# Validate the checksums for a single file ingested into the jade repo.
# Log an error if the checksums do not match.
def validate_checksum(load_history_row):
    target_path = load_history_row["target_path"]
    jade_checksum = load_history_row["checksum_crc32c"]

    # check that the original checksum matches the new one
    original_checksum = target_path.split("/")[-1]
    if not (original_checksum == jade_checksum):
        log_checksum_error(load_history_row["source_name"], target_path)


# Query the BQ API for summary information about each successful file load.
sql_query = f"""
    SELECT source_name, target_path, checksum_crc32c
    FROM `{project_id}.{dataset_name}.datarepo_load_history`
    WHERE state='succeeded'
    AND load_tag='{load_tag}'
    """
query_job = bqclient.query(sql_query)

for result_row in query_job:
    validate_checksum(result_row)
