set -euo pipefail

# Staged requests have: source_path, target_path, crc32c
# Jade table has: checksum_crc32c, error in 'datarepo_load_history'

declare -r TABLE=file_load_requests

declare -ra BQ_QUERY=(
  bq
  --location=US
  --project_id=${STAGING_PROJECT}
  --synchronous_mode=true
  --headless=true
  --format=none
  query
  --use_legacy_sql=false
  --replace=true
  --external_table_definition=${TABLE}::source_path:STRING,target_path:STRING,crc32c:STRING@NEWLINE_DELIMITED_JSON=${GCS_PREFIX}/*
  --destination_table=${STAGING_PROJECT}:${STAGING_DATASET}.${TARGET_TABLE}
)

1>&2 ${BQ_QUERY[@]} "SELECT S.source_path AS source_path, S.target_path AS target_path
  FROM ${TABLE} S LEFT OUTER JOIN \`${JADE_PROJECT}.${JADE_DATASET}.datarepo_load_history\` J
  ON S.crc32c = J.checksum_crc32c AND J.error IS NULL
  WHERE J.checksum_crc32c IS NULL"

# Echo the output table name so Argo can slurp it into a parameter.
echo ${TARGET_TABLE}
