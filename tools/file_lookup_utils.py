#!/usr/bin/env python3
"""
File lookup utilities for the runtime container.
Handles subdirectory-based file lookups for optimal filesystem performance.
"""

import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class FileLookupManager:
    """Manages file lookups in the subdirectory structure."""

    def __init__(self, data_dir: Path, mapping_file: Optional[Path] = None):
        """
        Initialize the file lookup manager.

        Args:
            data_dir: Base directory containing processed data
            mapping_file: Optional path to file_path_mapping.json
        """
        self.data_dir = Path(data_dir)
        self.mapping_file = mapping_file or (self.data_dir / "file_path_mapping.json")
        self.file_mapping = self._load_file_mapping()
        self.subdirectory_depth = self.file_mapping.get("subdirectory_depth", 2)

    def _load_file_mapping(self) -> Dict[str, Any]:
        """Load the file path mapping."""
        if self.mapping_file.exists():
            try:
                with open(self.mapping_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load file mapping: {e}")

        # Fallback to empty mapping
        return {
            "artists": {},
            "albums": {},
            "subdirectory_depth": 2,
            "base_paths": {
                "artists": str(self.data_dir / "artist"),
                "albums": str(self.data_dir / "album")
            }
        }

    def get_artist_file_path(self, mbid: str) -> Optional[Path]:
        """
        Get the file path for an artist MBID.

        Args:
            mbid: The artist MBID

        Returns:
            Path to the artist file, or None if not found
        """
        # Try mapping first
        if mbid in self.file_mapping.get("artists", {}):
            relative_path = self.file_mapping["artists"][mbid]
            full_path = self.data_dir / relative_path
            if full_path.exists():
                return full_path

        # Fallback to subdirectory calculation
        return self._calculate_artist_path(mbid)

    def get_album_file_path(self, mbid: str) -> Optional[Path]:
        """
        Get the file path for an album MBID.

        Args:
            mbid: The album MBID

        Returns:
            Path to the album file, or None if not found
        """
        # Try mapping first
        if mbid in self.file_mapping.get("albums", {}):
            relative_path = self.file_mapping["albums"][mbid]
            full_path = self.data_dir / relative_path
            if full_path.exists():
                return full_path

        # Fallback to subdirectory calculation
        return self._calculate_album_path(mbid)

    def _calculate_artist_path(self, mbid: str) -> Optional[Path]:
        """Calculate artist file path using subdirectory structure."""
        artist_dir = self.data_dir / "artist"

        # Create subdirectory path
        subdirs = []
        for i in range(self.subdirectory_depth):
            start = i * 2
            end = start + 2
            if end <= len(mbid):
                subdirs.append(mbid[start:end])

        # Build the full path
        file_path = artist_dir
        for subdir in subdirs:
            file_path = file_path / subdir

        file_path = file_path / f"{mbid}.json"

        return file_path if file_path.exists() else None

    def _calculate_album_path(self, mbid: str) -> Optional[Path]:
        """Calculate album file path using subdirectory structure."""
        album_dir = self.data_dir / "album"

        # Create subdirectory path
        subdirs = []
        for i in range(self.subdirectory_depth):
            start = i * 2
            end = start + 2
            if end <= len(mbid):
                subdirs.append(mbid[start:end])

        # Build the full path
        file_path = album_dir
        for subdir in subdirs:
            file_path = file_path / subdir

        file_path = file_path / f"{mbid}.json"

        return file_path if file_path.exists() else None

    def load_artist(self, mbid: str) -> Optional[Dict[str, Any]]:
        """
        Load artist data by MBID.

        Args:
            mbid: The artist MBID

        Returns:
            Artist data dictionary, or None if not found
        """
        file_path = self.get_artist_file_path(mbid)
        if not file_path:
            return None

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading artist {mbid}: {e}")
            return None

    def load_album(self, mbid: str) -> Optional[Dict[str, Any]]:
        """
        Load album data by MBID.

        Args:
            mbid: The album MBID

        Returns:
            Album data dictionary, or None if not found
        """
        file_path = self.get_album_file_path(mbid)
        if not file_path:
            return None

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading album {mbid}: {e}")
            return None

    def get_directory_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the file structure.

        Returns:
            Dictionary with directory statistics
        """
        stats = {
            "total_artists": len(self.file_mapping.get("artists", {})),
            "total_albums": len(self.file_mapping.get("albums", {})),
            "subdirectory_depth": self.subdirectory_depth,
            "base_paths": self.file_mapping.get("base_paths", {}),
            "artist_directories": 0,
            "album_directories": 0
        }

        # Count actual directories
        artist_base = self.data_dir / "artist"
        album_base = self.data_dir / "album"

        if artist_base.exists():
            stats["artist_directories"] = len(list(artist_base.rglob("*.json")))

        if album_base.exists():
            stats["album_directories"] = len(list(album_base.rglob("*.json")))

        return stats


def create_lookup_function(data_dir: str) -> FileLookupManager:
    """
    Create a file lookup manager for the runtime container.

    Args:
        data_dir: Path to the processed data directory

    Returns:
        FileLookupManager instance
    """
    return FileLookupManager(Path(data_dir))


# Example usage for FastAPI service
def example_fastapi_usage():
    """
    Example of how to use this in a FastAPI service.
    """
    from fastapi import FastAPI, HTTPException

    app = FastAPI()

    # Initialize lookup manager
    lookup_manager = create_lookup_function("/data/processed")

    @app.get("/artist/{mbid}")
    def get_artist(mbid: str):
        artist_data = lookup_manager.load_artist(mbid)
        if not artist_data:
            raise HTTPException(status_code=404, detail="Artist not found")
        return artist_data

    @app.get("/album/{mbid}")
    def get_album(mbid: str):
        album_data = lookup_manager.load_album(mbid)
        if not album_data:
            raise HTTPException(status_code=404, detail="Album not found")
        return album_data

    @app.get("/stats")
    def get_stats():
        return lookup_manager.get_directory_stats()


if __name__ == "__main__":
    # Test the lookup manager
    import sys

    if len(sys.argv) != 2:
        print("Usage: python file_lookup_utils.py <data_directory>")
        sys.exit(1)

    data_dir = sys.argv[1]
    lookup_manager = create_lookup_function(data_dir)

    # Print statistics
    stats = lookup_manager.get_directory_stats()
    print("File Lookup Manager Statistics:")
    print(f"  Total artists: {stats['total_artists']}")
    print(f"  Total albums: {stats['total_albums']}")
    print(f"  Subdirectory depth: {stats['subdirectory_depth']}")
    print(f"  Artist files found: {stats['artist_directories']}")
    print(f"  Album files found: {stats['album_directories']}")

    # Test a lookup
    test_mbid = "a74b1b7f-71a5-4011-9441-d0b5e4122711"  # Radiohead
    artist_path = lookup_manager.get_artist_file_path(test_mbid)
    print(f"\nTest lookup for {test_mbid}:")
    print(f"  Artist file path: {artist_path}")
    if artist_path:
        print(f"  File exists: {artist_path.exists()}")
