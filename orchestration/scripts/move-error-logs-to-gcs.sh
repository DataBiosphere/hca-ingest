set -euo pipefail

declare -r LOGS_DIR=../../logs
declare -r LOG_FILE=${LOGS_DIR}/errors.log
declare -r NEW_FILENAME=${LOGS_DIR}/${TIMESTAMP}.log

if test -f "$LOG_FILE"; then
  # rename the file to the start time of the import
  mv ${LOG_FILE} ${NEW_FILENAME}
  # move the file to a bucket
  gsutil cp ${NEW_FILENAME} ${BUCKET_NAME}/errors/
  rm ${NEW_FILENAME}
fi

