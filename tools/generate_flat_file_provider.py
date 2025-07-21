#!/usr/bin/env python3
"""
Generate flat file provider structure from normalized data.
Creates the directory structure and search indexes for direct serving.
"""

import json
import sqlite3
import time
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FlatFileProviderGenerator:
    """Generate flat file provider structure from normalized data."""

    def __init__(self, provider_dir: Path):
        self.provider_dir = provider_dir
        self.artists_dir = provider_dir / "artists"
        self.albums_dir = provider_dir / "albums"
        self.search_dir = provider_dir / "search"

        # Create directories
        self.artists_dir.mkdir(parents=True, exist_ok=True)
        self.albums_dir.mkdir(parents=True, exist_ok=True)
        self.search_dir.mkdir(parents=True, exist_ok=True)

    def generate_from_normalized(self, normalized_dir: Path) -> Dict[str, Any]:
        """Generate flat files from normalized data."""
        logger.info(f"Generating flat file provider from {normalized_dir}")
        start_time = time.time()

        stats = {
            "artists_processed": 0,
            "albums_processed": 0,
            "files_copied": 0,
            "search_indexes_created": 0,
            "errors": [],
            "processing_time": 0
        }

        try:
            # Step 1: Copy artist files
            logger.info("Copying artist files...")
            artist_stats = self._copy_artist_files(normalized_dir)
            stats.update(artist_stats)

            # Step 2: Copy album files
            logger.info("Copying album files...")
            album_stats = self._copy_album_files(normalized_dir)
            stats.update(album_stats)

            # Step 3: Create search indexes
            logger.info("Creating search indexes...")
            search_stats = self._create_search_indexes()
            stats.update(search_stats)

            # Step 4: Generate statistics
            logger.info("Generating provider statistics...")
            provider_stats = self._generate_provider_stats()
            stats.update(provider_stats)

        except Exception as e:
            logger.error(f"Error generating flat file provider: {e}")
            stats["errors"].append(str(e))

        stats["processing_time"] = time.time() - start_time
        return stats

    def _copy_artist_files(self, normalized_dir: Path) -> Dict[str, Any]:
        """Copy artist files from normalized data."""
        stats = {
            "artists_processed": 0,
            "artist_files_copied": 0,
            "artist_errors": []
        }

        # Find all normalized artist files
        artist_files = list(normalized_dir.glob("*_normalized.json"))

        for artist_file in artist_files:
            try:
                # Extract MBID from filename
                mbid = artist_file.stem.replace("_normalized", "")

                # Copy to provider directory
                target_file = self.artists_dir / f"{mbid}.json"
                shutil.copy2(artist_file, target_file)

                stats["artists_processed"] += 1
                stats["artist_files_copied"] += 1

                logger.debug(f"Copied artist: {mbid}")

            except Exception as e:
                logger.error(f"Error copying artist file {artist_file}: {e}")
                stats["artist_errors"].append(str(e))

        logger.info(f"Copied {stats['artist_files_copied']} artist files")
        return stats

    def _copy_album_files(self, normalized_dir: Path) -> Dict[str, Any]:
        """Copy album files from normalized data."""
        stats = {
            "albums_processed": 0,
            "album_files_copied": 0,
            "album_errors": []
        }

        # Find all album directories
        albums_dir = normalized_dir / "albums"
        if not albums_dir.exists():
            logger.warning(f"Albums directory not found: {albums_dir}")
            return stats

        # Process each artist's album directory
        for artist_dir in albums_dir.iterdir():
            if not artist_dir.is_dir():
                continue

            try:
                # Copy all album files for this artist
                for album_file in artist_dir.glob("*.json"):
                    target_file = self.albums_dir / album_file.name
                    shutil.copy2(album_file, target_file)

                    stats["albums_processed"] += 1
                    stats["album_files_copied"] += 1

                logger.debug(f"Copied albums for artist: {artist_dir.name}")

            except Exception as e:
                logger.error(f"Error copying albums for artist {artist_dir.name}: {e}")
                stats["album_errors"].append(str(e))

        logger.info(f"Copied {stats['album_files_copied']} album files")
        return stats

    def _create_search_indexes(self) -> Dict[str, Any]:
        """Create SQLite FTS5 search indexes."""
        stats = {
            "search_indexes_created": 0,
            "search_errors": []
        }

        try:
            # Create artist search index
            self._create_artist_search_index()
            stats["search_indexes_created"] += 1

            # Create album search index
            self._create_album_search_index()
            stats["search_indexes_created"] += 1

        except Exception as e:
            logger.error(f"Error creating search indexes: {e}")
            stats["search_errors"].append(str(e))

        return stats

    def _create_artist_search_index(self):
        """Create SQLite FTS5 index for artist search."""
        db_path = self.search_dir / "artists.db"

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create FTS5 virtual table
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS artists_fts
            USING fts5(
                id, name, sort_name, unaccented_name,
                genres, type, country
            )
        """)

        # Clear existing data
        cursor.execute("DELETE FROM artists_fts")

        # Insert artist data
        for artist_file in self.artists_dir.glob("*.json"):
            try:
                with open(artist_file, 'r', encoding='utf-8') as f:
                    artist_data = json.load(f)

                # Extract searchable fields
                artist_id = artist_data.get("id", "")
                name = artist_data.get("artistName", "")
                sort_name = artist_data.get("sortName", "")
                genres = ", ".join(artist_data.get("genres", []))
                artist_type = artist_data.get("type", "")

                # Create unaccented name (simple version)
                unaccented_name = self._remove_accents(name)

                cursor.execute("""
                    INSERT INTO artists_fts (id, name, sort_name, unaccented_name, genres, type, country)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (artist_id, name, sort_name, unaccented_name, genres, artist_type, ""))

            except Exception as e:
                logger.error(f"Error indexing artist {artist_file}: {e}")

        conn.commit()
        conn.close()

        logger.info(f"Created artist search index: {db_path}")

    def _create_album_search_index(self):
        """Create SQLite FTS5 index for album search."""
        db_path = self.search_dir / "albums.db"

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create FTS5 virtual table
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS albums_fts
            USING fts5(
                id, title, artist_id, artist_name,
                unaccented_title, genres, type, release_date
            )
        """)

        # Clear existing data
        cursor.execute("DELETE FROM albums_fts")

        # Insert album data
        for album_file in self.albums_dir.glob("*.json"):
            try:
                with open(album_file, 'r', encoding='utf-8') as f:
                    album_data = json.load(f)

                # Extract searchable fields
                album_id = album_data.get("id", "")
                title = album_data.get("title", "")
                genres = ", ".join(album_data.get("genres", []))
                album_type = album_data.get("type", "")
                release_date = album_data.get("releaseDate", "")

                # Create unaccented title
                unaccented_title = self._remove_accents(title)

                # For now, we'll need to get artist info from the artist files
                # This is a simplified version - in production we'd want to optimize this
                artist_id = ""
                artist_name = ""

                cursor.execute("""
                    INSERT INTO albums_fts (id, title, artist_id, artist_name, unaccented_title, genres, type, release_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (album_id, title, artist_id, artist_name, unaccented_title, genres, album_type, release_date))

            except Exception as e:
                logger.error(f"Error indexing album {album_file}: {e}")

        conn.commit()
        conn.close()

        logger.info(f"Created album search index: {db_path}")

    def _remove_accents(self, text: str) -> str:
        """Remove accents from text for better search matching."""
        # Simple accent removal - in production you might want a more sophisticated approach
        import unicodedata

        # Normalize unicode and remove combining characters
        normalized = unicodedata.normalize('NFD', text)
        return ''.join(c for c in normalized if not unicodedata.combining(c))

    def _generate_provider_stats(self) -> Dict[str, Any]:
        """Generate statistics about the provider."""
        stats = {
            "provider_stats": {}
        }

        # Count files
        artist_count = len(list(self.artists_dir.glob("*.json")))
        album_count = len(list(self.albums_dir.glob("*.json")))

        # Calculate sizes
        artist_size = sum(f.stat().st_size for f in self.artists_dir.glob("*.json"))
        album_size = sum(f.stat().st_size for f in self.albums_dir.glob("*.json"))

        # Database sizes
        search_size = 0
        for db_file in self.search_dir.glob("*.db"):
            search_size += db_file.stat().st_size

        total_size = artist_size + album_size + search_size

        stats["provider_stats"] = {
            "artist_files": artist_count,
            "album_files": album_count,
            "artist_size_mb": artist_size / (1024 * 1024),
            "album_size_mb": album_size / (1024 * 1024),
            "search_size_mb": search_size / (1024 * 1024),
            "total_size_mb": total_size / (1024 * 1024)
        }

        return stats

def main():
    """Main function to generate flat file provider."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate flat file provider from normalized data")
    parser.add_argument("--normalized-dir", default="local/normalized_data",
                       help="Directory containing normalized data")
    parser.add_argument("--provider-dir", default="local/provider_demo",
                       help="Directory to create provider files")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    normalized_dir = Path(args.normalized_dir)
    provider_dir = Path(args.provider_dir)

    if not normalized_dir.exists():
        logger.error(f"Normalized data directory not found: {normalized_dir}")
        sys.exit(1)

    # Generate flat file provider
    generator = FlatFileProviderGenerator(provider_dir)
    stats = generator.generate_from_normalized(normalized_dir)

    # Print results
    print("\n" + "="*80)
    print("FLAT FILE PROVIDER GENERATION COMPLETE")
    print("="*80)

    print(f"Processing Time: {stats['processing_time']:.2f} seconds")
    print(f"Artists Processed: {stats['artists_processed']}")
    print(f"Albums Processed: {stats['albums_processed']}")
    print(f"Files Copied: {stats['files_copied']}")
    print(f"Search Indexes Created: {stats['search_indexes_created']}")

    if stats.get("provider_stats"):
        ps = stats["provider_stats"]
        print(f"\nProvider Statistics:")
        print(f"  Artist Files: {ps['artist_files']}")
        print(f"  Album Files: {ps['album_files']}")
        print(f"  Total Size: {ps['total_size_mb']:.2f} MB")
        print(f"    - Artists: {ps['artist_size_mb']:.2f} MB")
        print(f"    - Albums: {ps['album_size_mb']:.2f} MB")
        print(f"    - Search: {ps['search_size_mb']:.2f} MB")

    if stats["errors"]:
        print(f"\nErrors: {len(stats['errors'])}")
        for error in stats["errors"][:5]:  # Show first 5 errors
            print(f"  - {error}")

    print(f"\nProvider directory: {provider_dir}")
    print("✅ Flat file provider generation complete!")

if __name__ == "__main__":
    main()
