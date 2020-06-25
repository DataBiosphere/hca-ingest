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

1>&2 ${BQ_QUERY[@]} "WITH J AS (
    SELECT target_path, checksum_crc32c FROM \`${JADE_PROJECT}.${JADE_DATASET}.datarepo_load_history\` WHERE state = 'succeeded'
  )
  SELECT S.source_path AS sourcePath, S.target_path AS targetPath
  FROM ${TABLE} S LEFT JOIN J ON J.state = 'succeeded' AND S.crc32c = J.checksum_crc32c AND S.target_path = J.target_path
  WHERE J.checksum_crc32c IS NULL"

# Echo the output table name so Argo can slurp it into a parameter.
echo ${TARGET_TABLE}
