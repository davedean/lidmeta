#!/usr/bin/env python3
"""
FastAPI search service for flat file provider.
Uses SQLite FTS5 indexes for fast artist and album search.
"""

import sqlite3
import json
import time
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MusicBrainz Flat File Search Service",
    description="Fast search service for MusicBrainz metadata using SQLite FTS5",
    version="1.0.0"
)

# Configuration for container environment
SEARCH_DB_PATH = Path(os.getenv("LMS_SEARCH_DB_PATH", "/data/processed/"))
PROCESSED_DATA_PATH = Path(os.getenv("LMS_PROCESSED_DATA_PATH", "/data/processed"))

def get_search_db_path() -> Path:
    """Get the search database path."""
    return SEARCH_DB_PATH

def get_processed_data_path() -> Path:
    """Get the processed data path."""
    return PROCESSED_DATA_PATH

def load_artist_data(mbid: str) -> Optional[Dict[str, Any]]:
    """Load full artist data from flat file structure."""
    # Path: /data/processed/artist/XX/YY/mbid.json
    xx = mbid[:2].lower()
    yy = mbid[2:4].lower()
    file_path = get_processed_data_path() / "artist" / xx / yy / f"{mbid}.json"

    if not file_path.exists():
        logger.warning(f"Artist file not found: {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading artist {mbid}: {e}")
        return None

def map_artist_for_search(artist_data: Dict[str, Any], score: int) -> Dict[str, Any]:
    """Map artist data to SearchResult format."""
    # The flat file format already matches SearchArtistInResult schema perfectly!
    # Just wrap it in the SearchResult structure
    return {
        "artist": artist_data,
        "album": None,
        "score": score
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "musicbrainz-search",
        "timestamp": time.time()
    }

@app.get("/search/artists")
async def search_artists(
    q: str = Query(..., description="Search query for artist name"),
    limit: int = Query(100, ge=1, le=100, description="Maximum number of results")
) -> List[Dict[str, Any]]:
    """Search artists and return schema-compliant SearchResult objects."""
    start_time = time.time()
    logger.info(f"Search request: q='{q}', limit={limit}")

    try:
        db_path = get_search_db_path() / "artist.db"
        logger.info(f"Database path: {db_path}")

        if not db_path.exists():
            logger.error(f"Database not found at: {db_path}")
            raise HTTPException(status_code=503, detail="Search index not available")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Use FTS5 for fast full-text search with BM25 ranking
        query = """
            SELECT id, name, sort_name, bm25(artists_fts) as rank
            FROM artists_fts
            WHERE artists_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """
        logger.info(f"Executing query: {query} with params: ({q}, {limit})")
        cursor.execute(query, (q, limit))
        raw_results = cursor.fetchall()
        logger.info(f"SQLite returned {len(raw_results)} results: {raw_results}")

        results = []
        for row in raw_results:
            artist_id, name, sort_name, bm25_rank = row
            logger.info(f"Processing artist: {artist_id} - {name} (BM25: {bm25_rank})")

            # Load full artist data from flat files
            artist_data = load_artist_data(artist_id)
            if artist_data:
                logger.info(f"Loaded artist data for {artist_id}")
                # Convert BM25 rank to score (BM25 is negative, lower is better)
                # Convert to 1-100+ scale where higher is better
                # Since BM25 is negative and lower is better, we need to invert it
                # Allow scores above 100 for very good matches
                base_score = max(1, int(100 - bm25_rank)) if bm25_rank else 50

                # Boost exact matches and name starts
                boost = 0
                query_lower = q.lower()
                name_lower = name.lower()

                # Import unidecode for accent-insensitive comparison
                try:
                    from unidecode import unidecode
                    query_unaccented = unidecode(query_lower)
                    name_unaccented = unidecode(name_lower)
                except ImportError:
                    # Fallback to lowercase comparison if unidecode not available
                    query_unaccented = query_lower
                    name_unaccented = name_lower

                # Exact name match gets highest boost (accent-insensitive)
                if name_unaccented == query_unaccented:
                    boost = 50
                    logger.info(f"  Exact name match: '{name}' == '{q}' (unaccented)")
                # Name starts with query gets high boost (accent-insensitive)
                elif name_unaccented.startswith(query_unaccented + " "):
                    boost = 30
                    logger.info(f"  Name starts with query: '{name}' starts with '{q} ' (unaccented)")
                # Name contains query as a word gets medium boost (accent-insensitive)
                elif f" {query_unaccented} " in f" {name_unaccented} ":
                    boost = 20
                    logger.info(f"  Name contains query as word: '{name}' contains '{q}' (unaccented)")
                # Name ends with query gets small boost (accent-insensitive)
                elif name_unaccented.endswith(" " + query_unaccented):
                    boost = 10
                    logger.info(f"  Name ends with query: '{name}' ends with ' {q}' (unaccented)")

                score = base_score + boost
                search_result = map_artist_for_search(artist_data, score)
                results.append(search_result)
                logger.info(f"Added result for {artist_id} with score {score} (BM25: {bm25_rank}, boost: {boost})")
            else:
                logger.warning(f"Could not load data for artist {artist_id}")

        # Sort results by final score descending so best matches appear first
        results.sort(key=lambda r: r["score"], reverse=True)

        conn.close()

        response_time = (time.time() - start_time) * 1000
        logger.info(f"Search completed: {len(results)} results in {response_time:.2f}ms")

        return results  # Direct return of SearchResult array

    except Exception as e:
        logger.error(f"Error searching artists: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.get("/api/v1/search")
async def search(
    type: str = Query("all", alias="type"),
    query: str = Query(..., min_length=1, alias="query")
) -> List[Dict[str, Any]]:
    """Lidarr-compatible search endpoint."""
    if type.lower() not in {"all", "artist"}:
        raise HTTPException(status_code=400, detail="Only 'artist' and 'all' search types supported")

    # For now, all searches return artist results
    return await search_artists(q=query, limit=100)


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get search service statistics."""
    try:
        stats = {
            "search_indexes": {},
            "total_size_mb": 0
        }

        # Check search database
        db_name = "artist.db"
        db_path = get_search_db_path() / db_name
        if db_path.exists():
            size_mb = db_path.stat().st_size / (1024 * 1024)
            stats["search_indexes"][db_name] = {
                "exists": True,
                "size_mb": round(size_mb, 2)
            }
            stats["total_size_mb"] = round(size_mb, 2)
        else:
            stats["search_indexes"][db_name] = {
                "exists": False,
                "size_mb": 0
            }

        return stats

    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=f"Stats failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn

    # Get port from environment or use default
    port = int(os.getenv("SEARCH_SERVICE_PORT", "8000"))

    logger.info(f"Starting search service on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
