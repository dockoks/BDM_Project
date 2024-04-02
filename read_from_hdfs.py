import io
from pywebhdfs.webhdfs import PyWebHdfsClient
from avro.datafile import DataFileReader
from avro.io import DatumReader
import json

# Set HDFS cluster connection details
HDFS_IP = '10.4.41.52'
HDFS_PORT = 9870
HDFS_USER = 'bdm'
HDFS_AVRO_PATH = '/user/bdm'  # Adjust the path as per your HDFS setup

def read_avro_file_contents_from_hdfs(filename):
    # Create a PyWebHdfsClient instance
    client = PyWebHdfsClient(host=HDFS_IP, port=HDFS_PORT, user_name=HDFS_USER, timeout=None)

    # Read Avro file from HDFS
    hdfs_file = client.read_file(f"{HDFS_AVRO_PATH}/{filename}")

    # Wrap the bytes object in a seekable file-like object
    hdfs_file_io = io.BytesIO(hdfs_file)

    reader = DataFileReader(hdfs_file_io, DatumReader())

    # Retrieve Avro schema
    avro_schema_str = reader.meta.get('avro.schema')
    avro_schema = json.loads(avro_schema_str)

    # Ensure Avro schema is properly structured
    if 'fields' not in avro_schema:
        raise ValueError("Avro schema is not properly structured.")

    # Print Avro schema for debugging purposes
    print(f"Avro Schema for {filename}:")
    print(avro_schema)

    # Read Avro data
    avro_data = [record for record in reader]

    reader.close()
    return avro_schema, avro_data

def main():
    avro_files_data = {}  # Dictionary to store Avro file data

    # Create a PyWebHdfsClient instance
    client = PyWebHdfsClient(host=HDFS_IP, port=HDFS_PORT, user_name=HDFS_USER, timeout=None)

    # Get a list of all files in the directory
    files = client.list_dir(HDFS_AVRO_PATH)['FileStatuses']['FileStatus']

    # Filter out the directories, leaving only the files
    avro_files = [file['pathSuffix'] for file in files if file['type'] == 'FILE' and file['pathSuffix'].endswith('.avro')]

    # Read each Avro file
    for avro_file in avro_files:
        avro_schema, avro_data = read_avro_file_contents_from_hdfs(avro_file)
        avro_files_data[avro_file] = {'schema': avro_schema, 'data': avro_data}

    return avro_files_data

if __name__ == "__main__":
    avro_files_data = main()
    print(avro_files_data)  # For testing
