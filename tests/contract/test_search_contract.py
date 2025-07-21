import json
from pathlib import Path

from jsonschema import validate

from schema_validator.validate import get_schema_part

FIXTURE_DIR = Path(__file__).resolve().parent.parent.parent / "schema_validator" / "reference_payloads" / "search"


def _iter_payloads():
    if not FIXTURE_DIR.exists():
        return []
    return FIXTURE_DIR.glob("*.json")


for path in _iter_payloads():
    def _make_test(p):
        def _test():
            data = json.loads(p.read_text())
            schema = get_schema_part("#/components/schemas/SearchArtistArray")
            validate(instance=data, schema=schema)
        return _test
    globals()[f"test_search_schema_{path.stem}"] = _make_test(path)
