import json
import yaml
import argparse
from pathlib import Path
from jsonschema import validate, RefResolver

def validate_json_against_schema(json_file_path: Path, schema_file_path: Path, schema_name: str):
    """
    Validates a JSON file against a specific schema within an OpenAPI specification.

    Args:
        json_file_path (Path): The path to the JSON file to validate.
        schema_file_path (Path): The path to the OpenAPI YAML schema file.
        schema_name (str): The name of the schema component (e.g., 'ArtistResource') to validate against.
    """
    try:
        # Load the JSON data
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        print(f"✅ Successfully loaded JSON file: {json_file_path}")

        # Load the OpenAPI schema
        with open(schema_file_path, 'r', encoding='utf-8') as f:
            schema_data = yaml.safe_load(f)
        print(f"✅ Successfully loaded OpenAPI schema: {schema_file_path}")

        # Create a resolver to handle local $ref references
        resolver = RefResolver.from_schema(schema_data)

        # Extract the specific schema definition we want to validate against
        schema_definition = schema_data['components']['schemas'][schema_name]

        # Validate the JSON data against the schema
        validate(instance=json_data, schema=schema_definition, resolver=resolver)

        print(f"\n🎉 SUCCESS: {json_file_path} is valid against the {schema_name} schema.")
        return True

    except FileNotFoundError as e:
        print(f"❌ ERROR: File not found - {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: Invalid JSON in {json_file_path} - {e}")
        return False
    except yaml.YAMLError as e:
        print(f"❌ ERROR: Invalid YAML in {schema_file_path} - {e}")
        return False
    except Exception as e:
        print(f"❌ VALIDATION FAILED for {json_file_path}:")
        print(e)
        return False

def validate_file(json_file, component_name, schema_file=None):
    """Validates a JSON file against a component in an OpenAPI schema."""
    try:
        # Load the JSON data
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        print(f"✅ Successfully loaded JSON file: {json_file}")

        # Load the OpenAPI schema
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_data = yaml.safe_load(f)
        print(f"✅ Successfully loaded OpenAPI schema: {schema_file}")

        # Create a resolver to handle local $ref references
        resolver = RefResolver.from_schema(schema_data)

        # Extract the specific schema definition we want to validate against
        schema_definition = schema_data['components']['schemas'][component_name]

        # Validate the JSON data against the schema
        validate(instance=json_data, schema=schema_definition, resolver=resolver)

        print(f"\n🎉 SUCCESS: {json_file} is valid against the {component_name} schema.")
        return True

    except FileNotFoundError as e:
        print(f"❌ ERROR: File not found - {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: Invalid JSON in {json_file} - {e}")
        return False
    except yaml.YAMLError as e:
        print(f"❌ ERROR: Invalid YAML in {schema_file} - {e}")
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        return False


def get_schema_part(ref: str, schema_path: Path = Path(__file__).with_name("lidarr_api.yaml")):
    """
    Return a subschema (using a JSON-pointer like '#/components/schemas/Foo')
    so tests can call validate() directly.
    """
    import yaml, jsonpointer
    data = yaml.safe_load(schema_path.read_text())
    return jsonpointer.resolve_pointer(data, ref.lstrip("#"))


def main():
    parser = argparse.ArgumentParser(description="Validate a JSON file against a component schema in an OpenAPI spec.")
    parser.add_argument("json_file", type=str, help="Path to the JSON file to validate.")
    parser.add_argument("schema_name", type=str, help="The name of the schema in the components/schemas section (e.g., ArtistResource).")
    parser.add_argument("--schema_file", type=str, default="schema_validator/lidarr_api.yaml", help="Path to the OpenAPI YAML file.")

    args = parser.parse_args()

    json_path = Path(args.json_file)
    schema_path = Path(args.schema_file)

    if validate_json_against_schema(json_path, schema_path, args.schema_name):
        exit(0)
    else:
        exit(1)

if __name__ == "__main__":
    main()
