import pytest
import json
from pathlib import Path
import subprocess

# Define the root of the project to locate files easily
PROJECT_ROOT = Path(__file__).parent.parent

@pytest.fixture(scope="module")
def sample_artist_file():
    """Create a temporary, valid artist JSON file for testing."""
    output_dir = PROJECT_ROOT / "schema_validator" / "temp_output"
    output_dir.mkdir(exist_ok=True)

    artist_data = {
        "id": "5b11f4ce-a62d-471e-81fc-a69a9e9c3c8c",
        "artistname": "Test Artist",
        "sortname": "Artist, Test",
        "type": "Group",
        "status": "active",
        "overview": "A test artist.",
        "artistaliases": [],
        "genres": ["test"],
        "links": [],
        "images": [],
        "rating": {"count": 0, "value": 0.0},
        "oldids": [],
        "Albums": []
    }

    file_path = output_dir / "test_artist.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(artist_data, f)

    yield file_path

    # Teardown: clean up the file
    file_path.unlink()

def test_artist_schema_validation(sample_artist_file):
    """
    Tests that a valid artist JSON file passes validation against the OpenAPI spec.
    """
    schema_path = PROJECT_ROOT / "schema_validator" / "lidarr_api.yaml"
    validator_script = PROJECT_ROOT / "schema_validator" / "validate.py"

    # Ensure the script is executable, or call it via python
    command = [
        "python", str(validator_script),
        str(sample_artist_file),
        "ArtistResource",
        "--schema_file", str(schema_path)
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    # Print stdout/stderr for easier debugging in case of failure
    print("Validator STDOUT:", result.stdout)
    print("Validator STDERR:", result.stderr)

    assert result.returncode == 0, f"Validation script failed for {sample_artist_file}"
