#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
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

# Install requirements from requirements.txt
echo "Installing requirements..."
pip install -r requirements.txt > /dev/null
echo "Installed successfully"

# Run the Python scripts
echo "=========="
echo "Creating idealista avro file..."
python -c 'from json_folder_loader import make_idealista_avro; make_idealista_avro("'$DATA_PATH'")'
echo "Creation of idealista avro file completed"

echo "=========="
echo "Creating opendata avro file..."
python -c 'from csv_folder_loader import make_opendatabcn_avro; make_opendatabcn_avro("'$DATA_PATH'")'
echo "Creation of opendata avro file completed"

echo "=========="
echo "Creating lookup tables avro files..."
python -c 'from csv_loader import create_lookup_avros; create_lookup_avros("'$DATA_PATH'")'
echo "Creation of lookup tables avro files completed"

# Upload the Avro files to HDFS
echo "=========="
echo "Uploading Avro files to HDFS..."

# Loop through the Avro files in the directory and upload them
for file in "$AVRO_DATA_PATH"/*.avro; do
    if [[ -f "$file" ]]; then
        filename=$(basename "$file")
        echo "Uploading $filename to HDFS..."
        curl -L -i -X PUT "http://$HDFS_USER:$HDFS_PASSWORD@$HDFS_IP:$HDFS_PORT/webhdfs/v1$HDFS_AVRO_PATH/$filename?op=CREATE&overwrite=true" -T "$file"
    else
        echo "Error: File $file does not exist."
    fi
done

echo "=========="
echo "Upload process completed"
