from fastavro import writer, parse_schema
import os
import json

#extract the name of the file
def extract_date_from_filename(filename):
    base_name = os.path.basename(filename)
    date_part = base_name.split('_')[:3]
    return '-'.join(date_part)

#merge all the jsons files into one
def merge_jsons(directory_path):
    merged_data = []
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            file_path = os.path.join(directory_path, filename)
            # add new column publication date
            publication_date = extract_date_from_filename(filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                for entry in data:
                    entry['publication_date'] = publication_date
                merged_data.extend(data)
    return merged_data

# get data types
def get_datatypes(json_data):
    schema = {}
    for key, value in json_data.items():
        if isinstance(value, dict):
            schema[key] = get_datatypes(value)
        else:
            schema[key] = type(value).__name__
    return schema

#
def merge_json_schemas(schemas):
    merged_schema = {}
    for schema in schemas:
        for key, value in schema.items():
            if key not in merged_schema:
                merged_schema[key] = value
            elif isinstance(value, dict) and isinstance(merged_schema[key], dict):
                merged_schema[key] = merge_json_schemas([merged_schema[key], value])
    return merged_schema

def get_complete_json_schema(json_array):
    all_schemas = [get_datatypes(json_obj) for json_obj in json_array]
    return merge_json_schemas(all_schemas)

def convert_datatype(key, value_type):
    type_mapping = {
        "str": "string",
        "int": "int",
        "float": "float",
        "bool": "boolean"
    }
    if isinstance(value_type, dict):
        nested_schema = {
            "type": "record",
            "name": key.capitalize() + "Type",
            "fields": []
        }
        for nested_key, nested_value in value_type.items():
            nested_schema["fields"].append({"name": nested_key, "type": ["null", type_mapping[nested_value]]})
        return nested_schema
    else:
        return type_mapping[value_type]

def create_avro_schema(input_schema):
    avro_schema = {
        "type": "record",
        "name": "Idealista",
        "fields": []
    }
    for key, value in input_schema.items():
        field_schema = {"name": key, "type": ["null", convert_datatype(key, value)]}
        avro_schema["fields"].append(field_schema)
    return avro_schema

def make_idealista_avro(idealista_path='/Users/danilakokin/Desktop/UPC/Semester2/BDM/data'):
    merged_jsons = merge_jsons('{}/idealista'.format(idealista_path))
    out = get_complete_json_schema(merged_jsons)
    parsed_schema = parse_schema(create_avro_schema(out))
    json_data = merged_jsons

    avro_dir = os.path.join(idealista_path, 'avro')
    os.makedirs(avro_dir, exist_ok=True)

    avro_file_path = os.path.join(avro_dir, 'idealista.avro')

    with open(avro_file_path, 'wb') as out:
        writer(out, parsed_schema, json_data)
