import google.auth
from google.cloud import bigquery
import re

# TODO take values as arguments
project_id = ""
dataset_name = ""
load_tag = ""

# Set up BigQuery client
credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])
bqclient = bigquery.Client(credentials=credentials, project=project_id,)


def log_checksum_error(source_path: str, target_path: str):
    filename = re.search(r'([^/]*)$', source_path).group(1)
    error_log = {
        "errorType": "ChecksumError",
        "filePath": source_path,
        "fileName": filename,
        "message": f"Checksums do not match for the file ingested to ${target_path}."
    }
    log_file = open("../../logs/errors.log", "w")
    log_file.writelines(error_log)
    log_file.close()


def validate_load_history_row(row):
    target_path = row["target_path"]
    jade_checksum = row["checksum_crc32c"]

    # check that the original checksum matches the new one
    original_checksum = re.search(r'([^/]*)$', target_path).group(1)
    if not (original_checksum == jade_checksum):
        log_checksum_error(row["source_path"], target_path)


# Make a request to the BigQuery API
query_string = f"""
    SELECT source_name, target_path, checksum_crc32c 
    FROM `{project_id}.{dataset_name}.datarepo_load_history` 
    WHERE state='succeeded'
    AND load_tag='{load_tag}'
    LIMIT 10
    """
query_job = bqclient.query(query_string)

for result_row in query_job:
    validate_load_history_row(result_row)

# TODO test helper functions with fake data