#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the path
DATA_PATH=/Users/danilakokin/Desktop/UPC/Semester2/BDM/data

# Install requirements from requirements.txt
echo "Installing requirements..."
pip install -r requirements.txt

# Run the Python script
echo "Creating idealista avro file..."
python -c 'from idealista_loader import make_idealista_avro; make_idealista_avro('$IDEALISTA_PATH')'