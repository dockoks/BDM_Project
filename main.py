from fastavro import writer, reader, schema, parse_schema
import os
import json

def extract_date_from_filename(filename): #extract the name of the file
    base_name = os.path.basename(filename)
    date_part = base_name.split('_')[:3]
    return '-'.join(date_part)

def modify_and_merge_json(directory_path): #merge all the jsons files into one
    merged_data = []

    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            file_path = os.path.join(directory_path, filename)
            publication_date = extract_date_from_filename(filename) #add new column publication date
            with open(file_path, 'r') as file:
                data = json.load(file)
                for entry in data:
                    entry['publication_date'] = publication_date
                merged_data.extend(data)

    return merged_data

def get_schema_types(json_data):
    schema = {}
    for key, value in json_data.items():
        if isinstance(value, dict):
            schema[key] = get_schema_types(value)
        else:
            schema[key] = type(value).__name__
    return schema

def merge_schemas(schemas):
    merged_schema = {}
    for schema in schemas:
        for key, value in schema.items():
            if key not in merged_schema:
                merged_schema[key] = value
            elif isinstance(value, dict) and isinstance(merged_schema[key], dict):
                merged_schema[key] = merge_schemas([merged_schema[key], value])
    return merged_schema

def get_complete_schema(json_array):
    all_schemas = [get_schema_types(json_obj) for json_obj in json_array]
    return merge_schemas(all_schemas)

directory_path = '/Users/danilakokin/Desktop/UPC/Semester2/BDM/data/idealista'
merged_json_data = modify_and_merge_json(directory_path)

out = get_complete_schema(merged_json_data)
with open('.venv/data.json', 'w') as f:
    json.dump(out, f)

def create_avro_schema(input_schema):
    avro_schema = {
        "type": "record",
        "name": "RealEstateListing",
        "fields": []
    }

    for key, value in input_schema.items():
        field_schema = {"name": key, "type": ["null", convert_type(key, value)]}
        avro_schema["fields"].append(field_schema)

    return avro_schema

def convert_type(key, value_type):
    type_mapping = {
        "str": "string",
        "int": "int",
        "float": "float",
        "bool": "boolean"
    }

    if isinstance(value_type, dict):
        # Nested record
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

# Parse the schema
parsed_schema = parse_schema(create_avro_schema(out))

# Your JSON data
json_data = merged_json_data

# Serialize JSON data to Avro
with open('.venv/idealista.avro', 'wb') as out:
    writer(out, parsed_schema, json_data)
