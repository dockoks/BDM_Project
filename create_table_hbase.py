import json
import requests

HBASE_IP = '10.4.41.52'
HBASE_REST_PORT = 8080


def delete_hbase_table(table_name):
    # Make request to delete table using HBase REST API
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
    # Define table creation payload
    table_creation_payload = {
        "name": table_name,
        "ColumnSchema": []
    }

    # Iterate over column families
    for cf_name, cf_data in column_families.items():
        # Determine the value of VERSIONS based on max_versions
        versions = cf_data['max_versions']

        # Add column family with qualifiers and VERSIONS parameter
        column_family = {
            "name": cf_name,
            "VERSIONS": versions,  # Set VERSIONS parameter
            "qualifiers": cf_data['qualifiers']
        }
        table_creation_payload["ColumnSchema"].append(column_family)

    # Make request to create table using HBase REST API
    url = f'http://{HBASE_IP}:{HBASE_REST_PORT}/{table_name}/schema'
    headers = {'Content-Type': 'application/json'}
    response = requests.put(url, json=table_creation_payload, headers=headers)

    if response.status_code == 200 or response.status_code == 201:
        print(f"Created HBase table '{table_name}' successfully.")
    else:
        print(
            f"Failed to create HBase table '{table_name}': Status Code {response.status_code}, Response: {response.text}")


def main():
    # Read table structure from bigtable_structure.json
    with open('bigtables_structure.json', 'r') as json_file:
        tables = json.load(json_file)

    # Attempt to delete tables if they exist
    for table_name in tables.keys():
        delete_hbase_table(table_name)

    # Iterate over tables and create them in HBase
    for table_name, column_families in tables.items():
        create_hbase_table(table_name, column_families)


if __name__ == "__main__":
    main()
