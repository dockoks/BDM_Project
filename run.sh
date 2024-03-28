#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the path
DATA_PATH='/Users/danilakokin/Desktop/UPC/Semester2/BDM/BDM_Project/data'

# Install requirements from requirements.txt
echo "Installing requirements..."
pip install -r requirements.txt > /dev/null
echo "Installed successfully"

# Run the Python scripts
echo "=========="
echo "Creating idealista avro file..."
python -c 'from idealista_loader import make_idealista_avro; make_idealista_avro("'$DATA_PATH'")'
echo "Completed Python script for idealista"

echo "=========="
echo "Creating opendata avro file..."
python -c 'from opendatabcn_loader import make_opendatabcn_avro; make_opendatabcn_avro("'$DATA_PATH'")'
echo "Completed Python script for opendatabcn"