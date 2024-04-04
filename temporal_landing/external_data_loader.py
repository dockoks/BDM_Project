import requests
import pandas as pd
from io import StringIO
from temporal_landing.csv_folder_loader import generate_avro_schema_from_df, write_dataframe_to_avro

def is_valid_header(line):
    return sum(1 for column in line.split(',') if not column.strip()) <= 1

def max_commas(file):
    return max(line.count(',') for line in file)

def load_csv_and_set_header(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to download the file. Status code: {response.status_code}")
        return None

    content = response.content.decode('utf-8')
    lines = content.splitlines()
    max_commas_in_csv = max_commas(lines)

    for i, line in enumerate(lines):
        if line.count(',') == max_commas_in_csv and is_valid_header(line):
            df = pd.read_csv(StringIO(content), skiprows=i)
            return df
    return None

def create_external_avro(
        data_path,
        url='https://www.idescat.cat/pub/?id=irpf&n=4070&geo=mun:080193&lang=en&f=csv',
        filename='personal_income_tax'
    ):
    df = load_csv_and_set_header(url)
    if df is None:
        print("Failed to process the CSV data.")
        return

    avro_schema = generate_avro_schema_from_df(df, filename)
    write_dataframe_to_avro(df, avro_schema, data_path, filename)