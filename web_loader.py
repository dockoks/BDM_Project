import requests
import pandas as pd
from io import StringIO


def load_csv_and_set_header(url):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content.decode('utf-8')
        # Automatically determine the line number where data starts
        lines = content.splitlines()
        for i, line in enumerate(lines):
            if line.split(',')[0].strip().isdigit():
                header_line = i - 1
                break
        df = pd.read_csv(StringIO(content), skiprows=header_line)
        return df
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
        return None


url = 'https://www.idescat.cat/pub/?id=rfdbc&n=13301&geo=mun:080193&lang=en&f=csv'
df_cleaned = load_csv_and_set_header(url)

if df_cleaned is not None:
    print(df_cleaned.head())
else:
    print("Failed to process the CSV data.")
