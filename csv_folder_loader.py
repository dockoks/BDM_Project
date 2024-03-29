import os
import pandas as pd
import numpy as np
from fastavro import writer, parse_schema, reader

pd.set_option('future.no_silent_downcasting', True)

def make_empty_df(filepath):
    all_columns = set()
    for filename in os.listdir(filepath):
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join(filepath, filename), encoding='utf-8')
            all_columns.update(df.columns)

    all_columns = sorted(list(all_columns))
    combined_df = pd.DataFrame(columns=all_columns)
    return combined_df

# Process each CSV file
def process_csvs(filepath, combined_df):
    combined_df
    for filename in os.listdir(filepath):
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join(filepath, filename), encoding='utf-8')
            df = df.reindex(columns=combined_df.columns, fill_value=None)
            df = df.drop_duplicates()
            combined_df = pd.concat([combined_df, df], ignore_index=True)

    combined_df = combined_df.drop_duplicates()
    return combined_df

def infer_avro_type(value):
    if pd.isna(value):
        return 'null'
    elif isinstance(value, bool):
        return 'boolean'
    elif isinstance(value, int):
        return 'int'
    elif isinstance(value, float):
        return 'float'
    elif isinstance(value, str):
        return 'string'
    else:
        return 'string'

def generate_avro_schema_from_df(df, name):
    schema = {
        'type': 'record',
        'name': name,
        'fields': []
    }

    for column in df.columns:
        unique_types = {infer_avro_type(value) for value in df[column]}
        unique_types.add('null')  # Explicitly add 'null' to each field
        if len(unique_types) == 1 and 'null' in unique_types:
            field_type = 'null'
        else:
            field_type = sorted(list(unique_types))

        field = {
            'name': column,
            'type': field_type
        }
        schema['fields'].append(field)

    return schema

def dataframe_to_avro(df, avro_schema, data_path, filename):
    parsed_schema = parse_schema(avro_schema)
    records = df.to_dict(orient='records')
    avro_dir = os.path.join(data_path, 'avro')
    os.makedirs(avro_dir, exist_ok=True)
    avro_file_path = os.path.join(avro_dir, f'{filename}.avro')

    with open(avro_file_path, 'wb') as out:
        writer(out, parsed_schema, records)

def read_avro_to_dataframe(avro_file_path):
    if not os.path.exists(avro_file_path):
        return pd.DataFrame()

    records = []
    with open(avro_file_path, 'rb') as avro_file:
        for record in reader(avro_file):
            records.append(record)

    return pd.DataFrame(records)

def reconcile_schemas(existing_df, new_df):
    merged_columns = set(existing_df.columns).union(set(new_df.columns))
    reconciled_df = pd.concat([existing_df, new_df], ignore_index=True).reindex(columns=merged_columns).fillna(np.nan)
    reconciled_df = reconciled_df.infer_objects(copy=False)
    return reconciled_df

def make_opendatabcn_avro(data_path, name='opendatabcn-income'):
    avro_dir = os.path.join(data_path, 'avro')
    os.makedirs(avro_dir, exist_ok=True)
    formatted_name = name.replace('-', '')
    avro_file_path = os.path.join(avro_dir, f'{formatted_name}.avro')

    existing_df = read_avro_to_dataframe(avro_file_path)
    empty_df = make_empty_df(os.path.join(data_path, name))
    new_combined_df = process_csvs(os.path.join(data_path, name), empty_df)

    combined_df = reconcile_schemas(existing_df, new_combined_df)

    avro_schema = generate_avro_schema_from_df(combined_df, formatted_name)
    dataframe_to_avro(combined_df, avro_schema, data_path, formatted_name)




