import requests
import base64
import io
from pywebhdfs.webhdfs import PyWebHdfsClient
from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
from tqdm import tqdm
import xml.etree.ElementTree as ET

HDFS_IP = '10.4.41.52'
HDFS_PORT = 9870
HDFS_USER = 'bdm'
HDFS_AVRO_PATH = '/user/bdm'  # Adjust the path as per your HDFS setup

HBASE_IP = '10.4.41.52'
HBASE_REST_PORT = 8080

def read_avro_file_contents_from_hdfs(filename):
    client = PyWebHdfsClient(host=HDFS_IP, port=HDFS_PORT, user_name=HDFS_USER, timeout=None)
    hdfs_file = client.read_file(f"{HDFS_AVRO_PATH}/{filename}")
    hdfs_file_io = io.BytesIO(hdfs_file)

    reader = DataFileReader(hdfs_file_io, DatumReader())
    avro_schema_str = reader.meta.get('avro.schema')
    avro_schema = json.loads(avro_schema_str)
    if 'fields' not in avro_schema:
        raise ValueError("Avro schema is not properly structured.")
    avro_data = [record for record in reader]

    reader.close()
    return avro_schema, avro_data

def get_table_names():
    url = f'http://{HBASE_IP}:{HBASE_REST_PORT}/'
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to retrieve table names: Status Code {response.status_code}, Response: {response.text}")
        return []

    # Decode the byte string and split it into a list
    table_names = response.content.decode('utf-8').strip().split('\n')
    return table_names

def delete_hbase_table(table_name):
    url = f'http://{HBASE_IP}:{HBASE_REST_PORT}/{table_name}/schema'
    response = requests.delete(url)

    if response.status_code == 200:
        print(f"Deleted HBase table '{table_name}' successfully.")
    elif response.status_code == 404:
        print(f"Table '{table_name}' does not exist.")
    else:
        print(
            f"Failed to delete HBase table '{table_name}': Status Code {response.status_code}, Response: {response.text}")


def create_hbase_table(table_name, column_families):
    table_creation_payload = {
        "name": table_name,
        "ColumnSchema": []
    }

    for cf_name, cf_data in column_families.items():

        column_family = {
            "name": cf_name,
            "VERSIONS": cf_data['max_versions'],
            "qualifiers": cf_data['qualifiers']
        }
        table_creation_payload["ColumnSchema"].append(column_family)

    url = f'http://{HBASE_IP}:{HBASE_REST_PORT}/{table_name}/schema'
    headers = {'Content-Type': 'application/json'}
    response = requests.put(url, json=table_creation_payload, headers=headers)

    if response.status_code == 200 or response.status_code == 201:
        print(f"Created HBase table '{table_name}' successfully.")
    else:
        print(
            f"Failed to create HBase table '{table_name}': Status Code {response.status_code}, Response: {response.text}")

def encode_data(value):
    return base64.b64encode(value.encode()).decode()

def insert_into_hbase_table(table_name, row_key, data):
    url = f'http://{HBASE_IP}:{HBASE_REST_PORT}/{table_name}/{row_key}'
    cell_data = []
    for column_family, columns in data.items():
        for qualifier, value in columns.items():
            cell = {
                "column": encode_data(f"{column_family}:{qualifier}"),
                "timestamp": 0,
                "$": encode_data(str(value))  # Ensure the value is a string
            }
            cell_data.append(cell)

    payload = {"Row": [{"key": encode_data(str(row_key)), "Cell": cell_data}]}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    response = requests.put(url, json=payload, headers=headers)

    if response.status_code not in [200, 201]:
        print(f"Failed to insert row key {row_key} into '{table_name}': {response.text}")


def process_avro_file_and_insert_data(table_name, avro_file, cf_structure, row_start_index=0):
    _, avro_data = read_avro_file_contents_from_hdfs(avro_file)
    record_count = len(avro_data)

    row_id_column = cf_structure.get('row_id')

    for record_index, record in tqdm(enumerate(avro_data, start=row_start_index), total=record_count, desc=f"Inserting data into {table_name}"):
        hbase_data = {}

        for cf_name, cf_info in cf_structure['families'].items():
            hbase_data[cf_name] = {qualifier: '' for qualifier in cf_info['qualifiers']}

        row_key = record_index
        if row_id_column and row_id_column in record:
            row_key = record[row_id_column]

        for key, value in record.items():
            if isinstance(value, dict) and key in cf_structure['families']:
                for nested_key, nested_value in value.items():
                    if nested_key in cf_structure['families'][key]['qualifiers']:
                        hbase_data[key][nested_key] = str(nested_value) if nested_value is not None else ''
            else:
                for cf_name, cf_info in cf_structure['families'].items():
                    if key in cf_info['qualifiers']:
                        hbase_data[cf_name][key] = str(value) if value is not None else ''

        insert_into_hbase_table(table_name, str(row_key), hbase_data)


def main():
    with open('persistent_landing/bigtables_structure.json', 'r') as json_file:
        tables_structure = json.load(json_file)

    print("==========")

    existing_table_names = set(get_table_names())
    configured_table_names = set(tables_structure.keys())

    tables_to_delete = existing_table_names - configured_table_names
    for table_name in tables_to_delete:
        delete_hbase_table(table_name)

    tables_to_create = configured_table_names - existing_table_names
    for table_name in tables_to_create:
        create_hbase_table(table_name, tables_structure[table_name]['families'])

    print("==========")

    client = PyWebHdfsClient(host=HDFS_IP, port=HDFS_PORT, user_name=HDFS_USER, timeout=None)
    files = client.list_dir(HDFS_AVRO_PATH)['FileStatuses']['FileStatus']
    avro_files = [f['pathSuffix'] for f in files if f['type'] == 'FILE' and f['pathSuffix'].endswith('.avro')]

    avro_files = avro_files[::-1]

    for avro_file in avro_files:
        table_name = avro_file.split('.')[0]
        if table_name in tables_structure:
            print(f"Processing file: {avro_file} for table: {table_name}")
            table_config = tables_structure[table_name]
            process_avro_file_and_insert_data(table_name, avro_file, table_config)

if __name__ == "__main__":
    main()
