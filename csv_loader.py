import pandas as pd
import glob
import os
from fastavro import writer, parse_schema
from csv_folder_loader import generate_avro_schema_from_df, write_dataframe_to_avro

def create_lookup_avros(data_path):
    csv_directory_path = f'{data_path}/lookup_tables'
    for csv_file_path in glob.glob(os.path.join(csv_directory_path, '*.csv')):
        df = pd.read_csv(csv_file_path, dtype=str)

        filename = os.path.splitext(os.path.basename(csv_file_path))[0]
        avro_schema = generate_avro_schema_from_df(df, filename)
        write_dataframe_to_avro(df, avro_schema, data_path, filename)