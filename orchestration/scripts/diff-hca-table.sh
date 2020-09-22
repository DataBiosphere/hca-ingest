set -euo pipefail

# Copy-pasted from StackOverflow.
function join_by { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }

# Point to BQ metadata we expect to be present on disk.
declare -r TABLE_DIR=/bq-metadata/${TABLE}
declare -r PK_COLS=$(cat ${TABLE_DIR}/primary-keys)
declare -r COMPARE_COLS=$(cat ${TABLE_DIR}/compare-cols)

# Build the WHERE clause of the SQL query.
declare -a COMPARISONS=()
for pk in ${PK_COLS//,/ }; do
  COMPARISONS+=("datarepo_${col} = S.${col}")
done
declare -r FULL_DIFF=$(join_by ' AND ' "${COMPARISONS[@]}")

declare -a DATAREPO_PKS=()
for col in ${PK_COLS//,/ }; do
  DATAREPO_PKS+=("J.${col} as datarepo_${col}")
done
declare -r REPO_KEYS=$(join_by ', ' "${DATAREPO_PKS[@]}")

declare -r TARGET_TABLE=${TABLE}_joined

# Target either the live view or the raw table.
# Targeting the raw table will prevent re-ingestion of rows that have already
# been appended & deleted.
declare JADE_TABLE
if [[ ${USE_RAW_TABLE} = 'true' ]]; then
  JADE_TABLE="\`${JADE_PROJECT}.${JADE_DATASET}.datarepo_raw_${TABLE}_*\`"
else
  JADE_TABLE="\`${JADE_PROJECT}.${JADE_DATASET}.${TABLE}\`"
fi

if [[ ! -z "${JADE_FILTER}" ]]; then
  JADE_TABLE="(SELECT * FROM ${JADE_TABLE} WHERE ${JADE_FILTER})"
fi

# Join the data staged in GCS against the existing Jade data, filtering out identical rows.
# The result is stored back in BigQuery for subsequent processing.
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
  --external_table_definition=${TABLE}::${TABLE_DIR}/schema.json@NEWLINE_DELIMITED_JSON=${GCS_PREFIX}/*
  --destination_table=${STAGING_PROJECT}:${STAGING_DATASET}.${TARGET_TABLE}
)
1>&2 ${BQ_QUERY[@]} "SELECT J.datarepo_row_id, S.*, ${REPO_KEYS}
  FROM ${TABLE} S FULL JOIN ${JADE_TABLE} J
  USING (${PK_COLS}) WHERE ${FULL_DIFF}"

# Echo the output table name so Argo can slurp it into a parameter.
echo ${TARGET_TABLE}
