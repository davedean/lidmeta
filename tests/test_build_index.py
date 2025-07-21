"""Tests for the index builder functionality."""

import json
import tempfile
from pathlib import Path

import pytest

from tools.build_index import build_index


class TestBuildIndex:
    """Test the index builder functionality."""

    def test_build_index_simple(self):
        """Test building an index from a simple NDJSON file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a simple NDJSON file
            dump_file = temp_path / "test.ndjson"
            with dump_file.open('w') as f:
                f.write('{"id": "artist1", "name": "Artist 1"}\n')
                f.write('{"id": "artist2", "name": "Artist 2"}\n')
                f.write('{"id": "artist3", "name": "Artist 3"}\n')

            # Build index
            index_file = temp_path / "test.idx"
            build_index(dump_file, index_file)

            # Verify index file exists and has correct size
            assert index_file.exists()
            # 3 entries * 24 bytes per entry = 72 bytes
            assert index_file.stat().st_size == 72

    def test_build_index_missing_id(self):
        """Test building index with entries missing 'id' field."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create NDJSON file with some entries missing 'id'
            dump_file = temp_path / "test.ndjson"
            with dump_file.open('w') as f:
                f.write('{"id": "artist1", "name": "Artist 1"}\n')
                f.write('{"name": "Artist without ID"}\n')  # Missing id
                f.write('{"id": "artist2", "name": "Artist 2"}\n')

            # Build index
            index_file = temp_path / "test.idx"
            build_index(dump_file, index_file)

            # Should only index entries with 'id' field
            assert index_file.exists()
            # 2 entries * 24 bytes per entry = 48 bytes
            assert index_file.stat().st_size == 48

    def test_build_index_invalid_json(self):
        """Test building index with invalid JSON lines."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create NDJSON file with invalid JSON
            dump_file = temp_path / "test.ndjson"
            with dump_file.open('w') as f:
                f.write('{"id": "artist1", "name": "Artist 1"}\n')
                f.write('invalid json line\n')  # Invalid JSON
                f.write('{"id": "artist2", "name": "Artist 2"}\n')

            # Build index
            index_file = temp_path / "test.idx"
            build_index(dump_file, index_file)

            # Should skip invalid lines and only index valid entries
            assert index_file.exists()
            # 2 entries * 24 bytes per entry = 48 bytes
            assert index_file.stat().st_size == 48
