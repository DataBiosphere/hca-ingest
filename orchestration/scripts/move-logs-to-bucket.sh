set -euo pipefail

declare -r LOGS_DIR=../../transformation/logs
declare -r NEW_FILENAME=${LOGS_DIR}/${TIMESTAMP}.log

# rename the file to the start time of the import, copy to a bucket
mv ${LOGS_DIR}/errors.log ${NEW_FILENAME}
gsutil cp ${NEW_FILENAME} ${BUCKET_NAME}/errors/