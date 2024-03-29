import io
from pywebhdfs.webhdfs import PyWebHdfsClient
from avro.datafile import DataFileReader
from avro.io import DatumReader
import requests
import json

# Set HDFS cluster connection details
HDFS_IP = '10.4.41.52'
HDFS_PORT = '9870'
HDFS_USER = 'bdm'
HDFS_PASSWORD = 'bdm'
HDFS_AVRO_PATH = '/user/bdm'  # Adjust the path as per your HDFS setup

# Set HBase connection details
HBASE_IP = '10.4.41.52'
HBASE_REST_PORT = 16010  # Default HBase REST port
HBASE_USER = 'bdm'
HBASE_PASSWORD = 'bdm'

def read_avro_file_schema_and_contents_from_hdfs(filename):
    # Create a PyWebHdfsClient instance
    client = PyWebHdfsClient(host=HDFS_IP, port=HDFS_PORT, user_name=HDFS_USER, timeout=None)

    # Read Avro file from HDFS
    hdfs_file = client.read_file(f"{HDFS_AVRO_PATH}/{filename}")

    # Wrap the bytes object in a seekable file-like object
    hdfs_file_io = io.BytesIO(hdfs_file)

    reader = DataFileReader(hdfs_file_io, DatumReader())

    # Print Avro schema
    print("Avro schema:")
    schema = reader.meta
    print(schema)
    reader.close()
    return schema

def create_hbase_table(schema):
    # Decode Avro schema bytes to string
    schema_str = schema['avro.schema'].decode('utf-8')

    # Extract table name from Avro schema
    table_name = schema_str.split('"name": "')[1].split('"')[0]

    # Construct the table creation statement with hardcoded vertical partitions
    table_creation_statement = f"create '{table_name}', {{ NAME => 'cf1', VERSIONS => '3' }}, {{ NAME => 'cf2', VERSIONS => '3' }}, {{ NAME => 'cf3', VERSIONS => '3' }}"

    # Create HBase table using REST API
    url = f'http://{HBASE_IP}:{HBASE_REST_PORT}/{table_name}/schema'
    headers = {'Content-Type': 'application/json'}
    payload = {
        "name": table_name,
        "ColumnSchema": [
            {"name": "cf1", "MAX_VERSIONS": 3},
            {"name": "cf2", "MAX_VERSIONS": 3},
            {"name": "cf3", "MAX_VERSIONS": 3}
        ]
    }
    response = requests.put(url, data=json.dumps(payload), headers=headers, auth=(HBASE_USER, HBASE_PASSWORD))

    if response.status_code == 200:
        print(f"Created HBase table '{table_name}' with vertical partitions.")
    else:
        print(f"Failed to create HBase table '{table_name}': {response.text}")

def main():
    # Read Avro file schema from HDFS
    print("Reading Avro file schema from HDFS...")
    schema = read_avro_file_schema_and_contents_from_hdfs('opendatabcnincome.avro')  # Change filename accordingly

    # Create HBase table based on Avro schema
    create_hbase_table(schema)

if __name__ == "__main__":
    main()
