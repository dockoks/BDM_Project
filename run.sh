#!/bin/bash

set -e

# Define the path
DATA_PATH='/Users/danilakokin/Desktop/UPC/Semester2/BDM/BDM_Project/data'
AVRO_DATA_PATH="$DATA_PATH/avro"

# Set HDFS cluster connection details
HDFS_IP='10.4.41.52'
HDFS_PORT='9870'
HDFS_USER='bdm'
HDFS_PASSWORD='bdm'
HDFS_AVRO_PATH='/user/bdm'

HBASE_IP='10.4.41.52'
HBASE_REST_PORT='8080'

# Install requirements from requirements.txt
echo "Installing requirements..."
pip install -r requirements.txt > /dev/null
echo "Installed successfully"

# Run the Python scripts
echo "=========="
echo "Creating idealista avro file..."
python -c 'from temporal_landing.json_folder_loader import make_idealista_avro; make_idealista_avro("'$DATA_PATH'")'
echo "Creation of idealista avro file completed"

echo "=========="
echo "Creating opendata avro file..."
python -c 'from temporal_landing.csv_folder_loader import make_opendatabcn_avro; make_opendatabcn_avro("'$DATA_PATH'")'
echo "Creation of opendata avro file completed"

echo "=========="
echo "Creating lookup tables avro files..."
python -c 'from temporal_landing.csv_loader import create_lookup_avros; create_lookup_avros("'$DATA_PATH'")'
echo "Creation of lookup tables avro files completed"

echo "=========="
echo "Creating external avro files..."
python -c 'from temporal_landing.external_data_loader import create_external_avro; create_external_avro("'$DATA_PATH'")'
echo "Creation of external avro files completed"

# TEMPORAL LENDING ZONE
echo "=========="
echo "Uploading Avro files to HDFS..."
python -c 'from temporal_landing.temporal_loader import upload_avro_to_hdfs; upload_avro_to_hdfs("'"$AVRO_DATA_PATH"'", "'"$HDFS_USER"'", "'"$HDFS_PASSWORD"'", "'"$HDFS_IP"'", "'"$HDFS_PORT"'", "'"$HDFS_AVRO_PATH"'")'
echo "Upload finished"

# PERSISTENT LENDING ZONE
echo "=========="
echo "Connecting to HBase..."
python -c 'from persistent_landing.persistent_loader import main; main()'
echo "Connection closed"
