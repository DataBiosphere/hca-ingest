import google.auth
from google.cloud import bigquery
import json
import os

# Take contextual information as arguments.
project_id = os.environ["PROJECT_ID"]
dataset_name = os.environ["DATASET_NAME"]
load_tag = os.environ["LOAD_TAG"]

# Set up BigQuery client.
credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])
bqclient = bigquery.Client(credentials=credentials, project=project_id,)


# Log a checksum error for the given source and target path.
def generate_checksum_error(load_history_row) -> str:
    source_name = load_history_row["source_name"]
    target_path = load_history_row["target_path"]
    new_checksum = load_history_row["checksum_crc32c"]
    error_log = {
        "errorType": "ChecksumError",
        "filePath": source_name,
        "fileName": source_name.split("/")[-1],
        "message": f"For the file ingested to {target_path}, the new checksum ({new_checksum}) "
                   "does not match the original checksum in the target path."
    }
    return json.dumps(error_log) + "\n"


# Query the BigQuery API
# For any files that have mis-matched checksums, query information for logging.
sql_query = f"""
    SELECT source_name, target_path, checksum_crc32c
    FROM `{project_id}.{dataset_name}.datarepo_load_history`
    WHERE state='succeeded'
    AND load_tag='{load_tag}'
    AND NOT ENDS_WITH(target_path, CONCAT('/', checksum_crc32c))
    """
query_result = bqclient.query(sql_query)
error_logs = map(generate_checksum_error, query_result)

with open('../../logs/errors.log', 'a+') as log_file:
    log_file.writelines(error_logs)
