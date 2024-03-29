import pandas as pd
import glob
import os
from fastavro import writer, parse_schema
from csv_folder_loader import generate_avro_schema_from_df

def create_lookup_avros(data_path):
    csv_directory_path = f'{data_path}/lookup_tables'
    for csv_file_path in glob.glob(os.path.join(csv_directory_path, '*.csv')):
        df = pd.read_csv(csv_file_path, dtype=str)

        filename = os.path.splitext(os.path.basename(csv_file_path))[0]
        schema = generate_avro_schema_from_df(df, filename)

        # Parse the schema
        parsed_schema = parse_schema(schema)

        avro_dir = os.path.join(data_path, 'avro')
        os.makedirs(avro_dir, exist_ok=True)
        avro_file_path = os.path.join(avro_dir, f'{filename}.avro')

        # Write to Avro file
        with open(avro_file_path, 'wb') as out:
            writer(out, parsed_schema, df.to_dict('records'))