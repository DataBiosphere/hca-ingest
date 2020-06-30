set -euo pipefail

declare -r JADE_TABLE=${JADE_PROJECT}.${JADE_DATASET}.${TABLE}
declare -r TARGET_TABLE=${TABLE}_outdated_ids

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
  --destination_table=${STAGING_PROJECT}:${STAGING_DATASET}.${TARGET_TABLE}
)
1>&2 ${BQ_QUERY[@]} "WITH latest_versions AS (
    SELECT ${TABLE}_id, MAX(version) AS latest_version
    FROM ${JADE_TABLE} GROUP BY ${TABLE}_id
  )
  SELECT J.datarepo_row_id FROM
  ${JADE_TABLE} J JOIN latest_versions L
  ON J.${TABLE}_id = L.${TABLE}_id
  WHERE J.version < L.latest_version"
