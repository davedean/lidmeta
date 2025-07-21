#!/usr/bin/env python3
"""Test search service mapper against schema validator."""

import json
import yaml
from pathlib import Path
from jsonschema import validate, RefResolver

def load_artist_data_dev(mbid: str):
    """Load artist data for development/testing."""
    # Development path: deploy/data/processed/artist/XX/YY/mbid.json
    xx = mbid[:2].lower()
    yy = mbid[2:4].lower()
    file_path = Path(f"../deploy/data/processed/artist/{xx}/{yy}/{mbid}.json")

    if not file_path.exists():
        print(f"File not found: {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading artist {mbid}: {e}")
        return None

def map_artist_for_search(artist_data, score):
    """Map artist data to SearchResult format."""
    return {
        "artist": artist_data,
        "album": None,
        "score": score
    }

def test_mapper_with_real_data():
    """Test the mapper with real artist data against the schema."""

    # Load schema
    schema_path = Path("../schema_validator/lidarr_api.yaml")
    with open(schema_path) as f:
        schema_doc = yaml.safe_load(f)

    resolver = RefResolver.from_schema(schema_doc)
    search_schema = schema_doc["components"]["schemas"]["SearchResultArray"]

    # Test with the example artist
    test_mbid = "00003ed9-32cb-4302-af5f-5b89bacdf557"

    # Load artist data using our function
    artist_data = load_artist_data_dev(test_mbid)

    if not artist_data:
        print(f"❌ Could not load artist data for {test_mbid}")
        return False

    print(f"✅ Loaded artist data: {artist_data['artistname']}")

    # Create SearchResult using our mapper
    search_result = map_artist_for_search(artist_data, score=95)

    # Validate single SearchResult
    single_result_schema = schema_doc["components"]["schemas"]["SearchResult"]
    validate(instance=search_result, schema=single_result_schema, resolver=resolver)
    print("✅ Single SearchResult validates against schema!")

    # Validate as array (what the API returns)
    results_array = [search_result]
    validate(instance=results_array, schema=search_schema, resolver=resolver)
    print("✅ SearchResult array validates against schema!")

    # Show the structure
    print("\n📋 Generated SearchResult structure:")
    print(json.dumps(search_result, indent=2)[:500] + "...")

    return True

if __name__ == "__main__":
    try:
        if test_mapper_with_real_data():
            print("\n🎉 All tests passed! Mapper implementation is schema-compliant.")
        else:
            print("\n❌ Tests failed!")
    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
