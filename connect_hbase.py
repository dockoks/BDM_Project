import happybase


def connect_to_hbase(host, port=9090):
    try:
        connection = happybase.Connection(host, port)
        connection.open()
        print("Connected to HBase on {}:{}".format(host, port))
        return connection
    except Exception as e:
        print("Failed to connect to HBase: ", e)
        return None


def list_hbase_tables(connection):
    if connection:
        tables = connection.tables()
        print("HBase Tables:", tables)


def main():
    host = '10.4.41.52'  # Replace with your HBase host
    port = 8080  # Replace with your HBase Thrift server port if it's not the default

    # Connect to HBase
    connection = connect_to_hbase(host, port)

    # List tables
    if connection:
        list_hbase_tables(connection)
        connection.close()
    else:
        print("No connection to HBase.")


if __name__ == "__main__":
    main()
