## Overview
This repository contains a comprehensive Big Data Management Backbone project, meticulously crafted by Danila Kokin (`danila.kokin@estudiantat.upc.edu`) and Dmitriy Chukhray (`dmitriy.chukhray@estudiantat.upc.edu`). It is designed to seamlessly integrate and process large-scale data, providing an efficient and automated data pipeline from collection to storage.

## Data Sources
Our primary data source is the `Personal Income Tax` table from the Statistical Institute of Catalonia, encompassing data from 2000-2021. This data is critical for our project's second phase, where we delve into descriptive statistical analysis and predictive modeling. 

- `Flexibility`: The data collection script (`external_data_loader.py`) is dynamically programmed, allowing for easy adaptation to different data sources from the institute’s website.

## Data Collectors
The data collection phase is powered by multiple Python scripts:

- `json_folder_loader.py`: Processes JSON files from Idealista, adding a `publication_date` column, and merges them into a single JSON object before converting to Avro format.
  
- `csv_folder_loader.py`: Similar to the JSON loader, this script works with CSV files, storing the combined data in a Pandas DataFrame, followed by Avro conversion.

- `csv_loader.py` & `temporal_loader.py`: These scripts facilitate the loading of lookup tables and the uploading of Avro files to the HDFS cluster.

## Data Persistent Loader
The project employs a single Python script alongside a JSON file (`bigtables_structure.json`) to design and create HBase big tables’ schemas.

- `persistent_loader.py`: This script orchestrates the creation of big tables in HBase, leveraging the REST API, and populates these tables with data from Avro files.

## Running Instructions
To execute the data pipeline:

1. Run the `run.sh` shell script if you are using a UNIX-like OS or `run_windows.sh` if you are on Windows (you will need Git Bash or a similar tool).
2. Configure the path to the folder containing the lookup tables, Idealista JSON files, and Opendatabcn CSV files.

You can clone the repository for all necessary resources: [GitHub Repository](https://github.com/dockoks/BDM_Project.git)

## Overall pipeline schema

![Diagram](images/diagram.svg)

## Contact

- Danila Kokin: `danila.kokin@estudiantat.upc.edu`
- Dmitriy Chukhray: `dmitriy.chukhray@estudiantat.upc.edu`
