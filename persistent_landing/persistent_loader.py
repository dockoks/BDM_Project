import requests
import base64
import io
import json
from pywebhdfs.webhdfs import PyWebHdfsClient
from avro.datafile import DataFileReader
from avro.io import DatumReader
from tqdm import tqdm

def read_avro_file_from_hdfs(hdfs_ip, hdfs_user, hdfs_port, hdfs_avro_path, filename):
    client = PyWebHdfsClient(host=hdfs_ip, port=hdfs_port, user_name=hdfs_user)
    hdfs_file = client.read_file(f"{hdfs_avro_path}/{filename}")
    hdfs_file_io = io.BytesIO(hdfs_file)

    reader = DataFileReader(hdfs_file_io, DatumReader())
    avro_schema = json.loads(reader.meta.get('avro.schema'))
    avro_data = [record for record in reader]
    reader.close()
    return avro_schema, avro_data

def get_table_names(hbase_ip, hbase_rest_port):
    response = requests.get(url=f'http://{hbase_ip}:{hbase_rest_port}/')
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve table names: {response.text}")
    return response.content.decode('utf-8').strip().split('\n')

def delete_hbase_table(hbase_ip, hbase_rest_port, table_name):
    response = requests.delete(url=f'http://{hbase_ip}:{hbase_rest_port}/{table_name}/schema')
    if response.status_code not in [200, 404]:
        raise Exception(f"Failed to delete HBase table '{table_name}': {response.text}")

def create_hbase_table(hbase_ip, hbase_rest_port, table_name, column_families):
    table_creation_payload = {
        "name": table_name,
        "ColumnSchema": [
            {
                "name": cf_name,
                "VERSIONS": cf_data['max_versions']
            } for cf_name, cf_data in column_families.items()
        ]
    }
    response = requests.put(
        url=f'http://{hbase_ip}:{hbase_rest_port}/{table_name}/schema',
        json=table_creation_payload,
        headers={'Content-Type': 'application/json'})
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create HBase table '{table_name}': {response.text}")

def encode_data(value):
    return base64.b64encode(value.encode()).decode()

def insert_into_hbase_table(hbase_ip, hbase_rest_port, table_name, row_key, data):
    cell_data = [
        {"column": encode_data(f"{cf}:{q}"), "$": encode_data(str(value))}
        for cf, columns in data.items() for q, value in columns.items()
    ]

    payload = {"Row": [{"key": encode_data(str(row_key)), "Cell": cell_data}]}
    response = requests.put(
        url=f'http://{hbase_ip}:{hbase_rest_port}/{table_name}/{row_key}',
        json=payload,
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
    )
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to insert row key {row_key} into '{table_name}': {response.text}")

def process_avro_file_and_insert_data(
        hdfs_ip,
        hdfs_user,
        hdfs_port,
        hdfs_avro_path,
        hbase_ip,
        hbase_rest_port,
        avro_file,
        table_name,
        cf_structure
    ):
    _, avro_data = read_avro_file_from_hdfs(hdfs_ip, hdfs_user, hdfs_port, hdfs_avro_path, avro_file)

    for record_index, record in tqdm(enumerate(avro_data), total=len(avro_data), desc=f"Inserting data into {table_name}"):
        hbase_data = {
            cf_name: {
                q: '' for q in cf_info['qualifiers']
            } for cf_name, cf_info in cf_structure['families'].items()
        }
        row_key = record.get(cf_structure.get('row_id'), record_index)
        for key, value in record.items():
            for cf_name, cf_info in cf_structure['families'].items():
                if key in cf_info['qualifiers']:
                    hbase_data[cf_name][key] = str(value) if value is not None else ''
        insert_into_hbase_table(hbase_ip, hbase_rest_port, table_name, row_key, hbase_data)

def load_hbase_tables(
        hdfs_ip,
        hdfs_user,
        hdfs_port,
        hdfs_avro_path,
        hbase_ip,
        hbase_rest_port
    ):
    with open('persistent_landing/bigtables_structure.json') as json_file:
        tables_structure = json.load(json_file)

    existing_table_names = set(get_table_names(hbase_ip, hbase_rest_port))
    configured_table_names = set(tables_structure.keys())

    tables_to_delete = existing_table_names - configured_table_names
    for table_name in tables_to_delete:
        delete_hbase_table(hbase_ip, hbase_rest_port, table_name)

    tables_to_create = configured_table_names - existing_table_names
    for table_name in tables_to_create:
        create_hbase_table(hbase_ip, hbase_rest_port, table_name, tables_structure[table_name]['families'])

    client = PyWebHdfsClient(host=hdfs_ip, port=hdfs_port, user_name=hdfs_user, timeout=200)

    file_statuses = client.list_dir(hdfs_avro_path)['FileStatuses']['FileStatus']
    avro_files = []
    for file_status in file_statuses:
        if file_status['type'] == 'FILE' and file_status['pathSuffix'].endswith('.avro'):
            avro_files.append(file_status['pathSuffix'])

    for avro_file in avro_files:
        table_name = avro_file.split('.')[0]
        if table_name in tables_structure:
            print(f"Processing file: {avro_file} for table: {table_name}")
            process_avro_file_and_insert_data(
                hdfs_ip, hdfs_user,
                hdfs_port,
                hdfs_avro_path,
                hbase_ip,
                hbase_rest_port,
                avro_file,
                table_name,
                tables_structure[table_name]
            )
