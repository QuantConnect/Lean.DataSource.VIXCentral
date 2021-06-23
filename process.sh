#!/bin/bash

# Gets the directory of the bash file itself so that we can call the relevant python file
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
TEMP_DIR="/temp-output-directory"
DESTINATION_DIR="${TEMP_DIR}/alternative/vixcentral"
exec_path=$(dirname $DIR)

mkdir -p $DESTINATION_DIR

python3 ${exec_path}/process_contango.py ${DESTINATION_DIR}

exit_code=$?
if [ $exit_code -ne 0 ];
then
    exit $exit_code
fi

echo "Uploading files to cache bucket"
aws s3 sync $TEMP_DIR s3://cache.quantconnect.com --no-progress

# Error in the spelling of the original, but maintain for backwards compatibility.
cp ${DESTINATION_DIR}/vix_contago.csv ${DESTINATION_DIR}/vix_contango.csv
