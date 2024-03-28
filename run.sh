#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the path
DATA_PATH='/Users/danilakokin/Desktop/UPC/Semester2/BDM/BDM_Project/data'
AVRO_DATA_PATH="$DATA_PATH/avro"

# Install requirements from requirements.txt
echo "Installing requirements..."
pip install -r requirements.txt > /dev/null
echo "Installed successfully"

AVRO_FILE1='idealista'
AVRO_FILE2='opendatabcn-income'

# Run the Python scripts
echo "=========="
echo "Creating idealista avro file..."
python -c 'from idealista_loader import make_idealista_avro; make_idealista_avro("'$DATA_PATH'", "'$AVRO_FILE1'")'
echo "Completed Python script for idealista"

echo "=========="
echo "Creating opendata avro file..."
python -c 'from opendatabcn_loader import make_opendatabcn_avro; make_opendatabcn_avro("'$DATA_PATH'", "'$AVRO_FILE2'")'
echo "Completed Python script for opendatabcn"

# Upload the Avro files to HDFS
echo "=========="
echo "Uploading Avro files to HDFS..."

# Set HDFS cluster connection details
HDFS_IP='10.4.41.52'
HDFS_PORT='9870'
HDFS_USER='bdm'
HDFS_PASSWORD='bdm'

HDFS_AVRO_PATH='/user/bdm'

# Use -L to make curl follow redirects
if [[ -f "$AVRO_DATA_PATH/$AVRO_FILE1.avro" ]]; then
    curl -L -i -X PUT "http://$HDFS_USER:$HDFS_PASSWORD@$HDFS_IP:$HDFS_PORT/webhdfs/v1$HDFS_AVRO_PATH/$AVRO_FILE1.avro?op=CREATE&overwrite=true" -T "$AVRO_DATA_PATH/$AVRO_FILE1.avro"
else
    echo "Error: File $AVRO_DATA_PATH/$AVRO_FILE1.avro does not exist."
fi

if [[ -f "$AVRO_DATA_PATH/$AVRO_FILE2.avro" ]]; then
    curl -L -i -X PUT "http://$HDFS_USER:$HDFS_PASSWORD@$HDFS_IP:$HDFS_PORT/webhdfs/v1$HDFS_AVRO_PATH/$AVRO_FILE2.avro?op=CREATE&overwrite=true" -T "$AVRO_DATA_PATH/$AVRO_FILE2.avro"
else
    echo "Error: File $AVRO_DATA_PATH/$AVRO_FILE2.avro does not exist."
fi

echo "Upload process completed"
