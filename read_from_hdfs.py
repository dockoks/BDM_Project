import io
from pywebhdfs.webhdfs import PyWebHdfsClient  # Import PyWebHdfsClient
from avro.datafile import DataFileReader
from avro.io import DatumReader
import happybase

# Set HDFS cluster connection details
HDFS_IP = '10.4.41.52'
HDFS_PORT = 9870
HDFS_USER = 'bdm'
HDFS_AVRO_PATH = '/user/bdm'  # Adjust the path as per your HDFS setup

# Set HBase connection details
HBASE_HOST = '10.4.41.52'
HBASE_PORT = 8080
HBASE_TABLE_NAME = 'opendatabcn_income'

def read_avro_file_contents_from_hdfs(filename):
    # Create a PyWebHdfsClient instance
    client = PyWebHdfsClient(host=HDFS_IP, port=HDFS_PORT, user_name=HDFS_USER, timeout=None)

    # Read Avro file from HDFS
    hdfs_file = client.read_file(f"{HDFS_AVRO_PATH}/{filename}")

    # Wrap the bytes object in a seekable file-like object
    hdfs_file_io = io.BytesIO(hdfs_file)

    reader = DataFileReader(hdfs_file_io, DatumReader())

    # Retrieve Avro schema
    avro_schema = reader.meta.get('avro.schema')

    # Read Avro data
    avro_data = [record for record in reader]

    reader.close()
    return avro_schema, avro_data

def main():
    # Read Avro file contents from HDFS
    avro_schema, avro_data = read_avro_file_contents_from_hdfs('opendatabcn_income.avro')  # Change filename accordingly

    # Print Avro schema
    print("Avro Schema:")
    print(avro_schema)

    # Print Avro data
    print("\nAvro Data:")
    for record in avro_data:
        print(record)

if __name__ == "__main__":
    main()