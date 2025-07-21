import os
import pytest
import json
from pathlib import Path
from schema_validator.validate import validate_file, get_schema_part
from jsonschema import validate
#from schema_validator.validate import


SCHEMA_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'lidarr_api.yaml'))
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

REFERENCE_PAYLOADS_DIR = os.path.join(PROJECT_ROOT, 'schema_validator', 'reference_payloads', 'direct')
SEARCH_PAYLOADS_DIR = os.path.join(PROJECT_ROOT, 'schema_validator', 'reference_payloads', 'search')

ARTIST_PAYLOADS_DIR = os.path.join(PROJECT_ROOT, 'deploy', 'data', 'processed', 'artist')
ALBUM_PAYLOADS_DIR = os.path.join(PROJECT_ROOT, 'deploy', 'data', 'processed', 'album')
#

# # Folder that now holds only search-array captures
# SEARCH_PAYLOAD_DIR = (
#     Path(__file__).resolve()
#     .parents[2]                 #  …/lidarr-metadata-server/
#     / "schema_validator"
#     / "reference_payloads"
#     / "search"
# )


def get_file_sample(directory, sample_size=5):
    """
    Returns a list of up to `sample_size` JSON files from a directory,
    searching through all subdirectories.
    Returns an empty list if the directory doesn't exist.
    """
    if not os.path.isdir(directory):
        return []

    json_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))

    # Sort for consistent sampling, then take the sample
    json_files.sort()
    return json_files[:sample_size]

@pytest.mark.parametrize("filename", get_file_sample(REFERENCE_PAYLOADS_DIR, 100)) # Test all reference files
def test_reference_payloads(filename):
    """Validates reference payloads from the live server against the OpenAPI schema."""
    file_path = filename # The function now returns the full path
    # Infer component name from filename (e.g., artist-radiohead.json -> ArtistResource)
    base_name = os.path.splitext(os.path.basename(filename))[0]
    component_type = base_name.split('-')[0]
    component_name = component_type.capitalize() + "Resource"
    assert validate_file(file_path, component_name, SCHEMA_FILE), f"Validation failed for reference payload: {filename}"

@pytest.mark.parametrize("filename", get_file_sample(ARTIST_PAYLOADS_DIR))
def test_generated_artists(filename):
    """Validates a sample of generated artist payloads against the ArtistResource schema."""
    file_path = filename # The function now returns the full path
    assert validate_file(file_path, "ArtistResource", SCHEMA_FILE), f"Validation failed for generated artist: {filename}"

@pytest.mark.parametrize("filename", get_file_sample(ALBUM_PAYLOADS_DIR))
def test_generated_albums(filename):
    """Validates a sample of generated album payloads against the AlbumResource schema."""
    file_path = filename # The function now returns the full path
    assert validate_file(file_path, "AlbumResource", SCHEMA_FILE), f"Validation failed for generated album: {filename}"

@pytest.mark.parametrize("filename", get_file_sample(SEARCH_PAYLOADS_DIR, 100))
def test_search_payloads(filename):
    """Validate /api/v1/search reference arrays."""
    data = json.loads(Path(filename).read_text())
    import yaml
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        full_schema = yaml.safe_load(f)
    from jsonschema import RefResolver
    resolver = RefResolver.from_schema(full_schema)
    schema = full_schema["components"]["schemas"]["SearchResultArray"]
    validate(instance=data, schema=schema, resolver=resolver)

# @pytest.mark.parametrize("payload_file", SEARCH_PAYLOADS_DIR.glob("*.json"))
# def test_search_payload_schema(payload_file):
#     """Search response must be an array of SearchArtistResult objects."""
#     data = json.loads(payload_file.read_text())
#     schema = get_schema_part("#/components/schemas/SearchArtistArray")
#     validate(instance=data, schema=schema)
