import requests
import json

HBASE_IP = '10.4.41.52'
HBASE_REST_PORT = 8080
HBASE_USER = 'bdm'  # Replace with your HBase username
HBASE_PASSWORD = 'bdm'  # Replace with your HBase password

def list_hbase_tables_rest():
    url = f'http://{HBASE_IP}:{HBASE_REST_PORT}/'
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers, auth=(HBASE_USER, HBASE_PASSWORD))

    if response.status_code == 200:
        tables = response.json()['table']
        print("HBase Tables:", tables)
    else:
        print("Failed to list HBase tables: Status Code", response.status_code)

def main():
    list_hbase_tables_rest()

if __name__ == "__main__":
    main()
