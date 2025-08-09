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
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import JSONResponse
import logging
import asyncio
from collections import OrderedDict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MusicBrainz Flat File Search Service",
    description="Fast search service for MusicBrainz metadata using SQLite FTS5",
    version="1.0.0"
)

"""Configurable settings via environment variables."""
SEARCH_DB_PATH = Path(os.getenv("LMS_SEARCH_DB_PATH", "/data/processed/"))
PROCESSED_DATA_PATH = Path(os.getenv("LMS_PROCESSED_DATA_PATH", "/data/processed"))

# Query gating and performance knobs
MIN_QUERY_LEN = int(os.getenv("LMS_MIN_QUERY_LEN", "3"))
DEBOUNCE_MS = int(os.getenv("LMS_DEBOUNCE_MS", "0"))  # 0 disables server-side debounce
FUZZY_MIN_LEN = int(os.getenv("LMS_FUZZY_MIN_LEN", "4"))
INNER_LIMIT_MULT = int(os.getenv("LMS_INNER_LIMIT_MULT", "10"))
INNER_LIMIT_MAX = int(os.getenv("LMS_INNER_LIMIT_MAX", "500"))

# Result cache to short-circuit repeat queries during typing
CACHE_TTL_MS = int(os.getenv("LMS_CACHE_TTL_MS", "10000"))  # 10s
CACHE_MAX_SIZE = int(os.getenv("LMS_CACHE_MAX_SIZE", "256"))

# In-memory structures
_active_requests = {}  # key -> { 'token': token }
_result_cache: "OrderedDict[str, tuple[float, list]]" = OrderedDict()

# Simple in-memory metrics
METRICS = {
    'requests_total': 0,
    'requests_active': 0,
    'requests_completed': 0,
    'short_queries': 0,
    'debounced_canceled': 0,
    'cache_hits': 0,
    'cache_misses': 0,
    'cancelled_during_processing': 0,
    'fuzzy_invocations': 0,
    'fuzzy_skipped_short': 0,
    'results_returned_total': 0,
    'execution_ms_total': 0.0
}


class CancellationToken:
    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


def _client_key(request: Optional[Request]) -> str:
    if request is None or request.client is None:
        return "no-client"
    ip = request.headers.get("x-forwarded-for") or request.client.host or "unknown"
    return f"{ip}:/search/artists"


def _cache_get(key: str, now_ms: float):
    entry = _result_cache.get(key)
    if not entry:
        return None
    ts_ms, data = entry
    if now_ms - ts_ms > CACHE_TTL_MS:
        # expired
        try:
            del _result_cache[key]
        except KeyError:
            pass
        return None
    # refresh LRU position
    _result_cache.move_to_end(key)
    return data


def _cache_put(key: str, data: list, now_ms: float):
    _result_cache[key] = (now_ms, data)
    _result_cache.move_to_end(key)
    # trim
    while len(_result_cache) > CACHE_MAX_SIZE:
        _result_cache.popitem(last=False)

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

def fuzzy_search_artists(cursor: sqlite3.Cursor, query: str, max_candidates: int = 500) -> List[tuple]:
    """
    Perform fuzzy search when FTS5 returns few results.
    Uses LIKE queries, string similarity, and metaphone matching to find close matches.
    """
    try:
        from rapidfuzz.fuzz import ratio
        from unidecode import unidecode
        from fuzzy import DMetaphone
        dm = DMetaphone()
    except ImportError as e:
        logger.warning(f"Required libraries not available ({e}), skipping fuzzy search")
        return []

    query_unaccented = unidecode(query.lower())
    query_words = query_unaccented.split()

    if not query_words:
        return []

    # Get query metaphone
    query_ascii = unidecode(query)
    query_metaphone = dm(query_ascii)[0] if dm(query_ascii)[0] else ""
    logger.info(f"Query metaphone: '{query}' -> '{query_ascii}' -> '{query_metaphone}'")

    # Use the first word as anchor for LIKE search to keep it fast
    anchor_word = query_words[0]

    # Get candidates that contain the anchor word OR have matching metaphone
    if query_metaphone:
        cursor.execute("""
            SELECT id, name, metaphone_primary, metaphone_secondary
            FROM artists_fts
            WHERE unaccented_name LIKE ? OR metaphone_primary = ? OR metaphone_secondary = ?
            LIMIT ?
        """, (f"%{anchor_word}%", query_metaphone, query_metaphone, max_candidates))
    else:
        cursor.execute("""
            SELECT id, name, metaphone_primary, metaphone_secondary
            FROM artists_fts
            WHERE unaccented_name LIKE ?
            LIMIT ?
        """, (f"%{anchor_word}%", max_candidates))

    candidates = cursor.fetchall()

    logger.info(f"Fuzzy search: found {len(candidates)} candidates for anchor word '{anchor_word}'")

    fuzzy_hits = []
    for artist_id, name, metaphone_primary, metaphone_secondary in candidates:
        name_unaccented = unidecode(name.lower())

        # Calculate string similarity score
        similarity = ratio(query_unaccented, name_unaccented)

        # Check metaphone match
        metaphone_match = False
        if query_metaphone:
            metaphone_match = (metaphone_primary == query_metaphone or
                             metaphone_secondary == query_metaphone)

        # Boost similarity if metaphone matches
        if metaphone_match:
            similarity = min(100, similarity + 15)  # Boost by 15 points
            logger.info(f"Metaphone match: '{name}' (metaphone: {metaphone_primary})")

        # Only include if similarity is above threshold
        if similarity >= 75:  # Adjustable threshold
            fuzzy_hits.append((artist_id, name, similarity))
            logger.info(f"Fuzzy match: '{name}' (similarity: {similarity:.1f}%, metaphone_match: {metaphone_match})")

    # Sort by similarity descending
    fuzzy_hits.sort(key=lambda x: x[2], reverse=True)

    logger.info(f"Fuzzy search: returning {len(fuzzy_hits)} matches above 75% similarity")
    return fuzzy_hits

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "musicbrainz-search",
        "timestamp": time.time()
    }

async def _search_artists_impl(
    q: str,
    limit: int,
    request: Optional[Request]
) -> List[Dict[str, Any]]:
    """Internal implementation for artist search with cancellation, cache and metrics."""
    start_time = time.time()
    logger.info(f"Search request: q='{q}', limit={limit}")

    token: Optional[CancellationToken] = None
    try:
        METRICS['requests_total'] += 1
        METRICS['requests_active'] += 1
        # Gate very short queries (cheap early return)
        if len(q.strip()) < MIN_QUERY_LEN:
            logger.info(f"Query below MIN_QUERY_LEN ({MIN_QUERY_LEN}), returning no results")
            METRICS['short_queries'] += 1
            return []

        # Optional server-side debounce to coalesce bursts
        token = CancellationToken()
        client_key = _client_key(request)

        # cancel any previous in-flight request for the same client
        previous = _active_requests.get(client_key)
        if previous and isinstance(previous.get('token'), CancellationToken):
            previous['token'].cancel()

        _active_requests[client_key] = { 'token': token }

        if DEBOUNCE_MS > 0:
            logger.debug(f"Debouncing {DEBOUNCE_MS}ms for client {client_key}")
            await asyncio.sleep(DEBOUNCE_MS / 1000.0)
            if token.cancelled:
                logger.info("Debounced request cancelled due to newer request; returning early")
                METRICS['debounced_canceled'] += 1
                return []

        # Cache check (case-insensitive)
        now_ms = time.time() * 1000
        cache_key = f"artists::{q.lower()}::{limit}"
        cached = _cache_get(cache_key, now_ms)
        if cached is not None:
            logger.info(f"Cache hit for '{q}' (limit {limit}), returning {len(cached)} results")
            METRICS['cache_hits'] += 1
            METRICS['results_returned_total'] += len(cached)
            METRICS['requests_completed'] += 1
            duration_ms = (time.time() - start_time) * 1000
            METRICS['execution_ms_total'] += duration_ms
            return cached
        else:
            METRICS['cache_misses'] += 1

        db_path = get_search_db_path() / "artist.db"
        logger.info(f"Database path: {db_path}")

        if not db_path.exists():
            logger.error(f"Database not found at: {db_path}")
            raise HTTPException(status_code=503, detail="Search index not available")

        conn = sqlite3.connect(db_path)
        # Install sqlite progress handler to allow cooperative cancellation
        def _progress_handler():
            return 1 if token.cancelled else 0
        # Invoke handler every N VDBE steps
        try:
            conn.set_progress_handler(_progress_handler, 1000)
        except Exception:
            # Not critical if unavailable
            pass
        cursor = conn.cursor()

        # Use internal limit to ensure we get enough candidates for boosting/sorting
        inner_limit = max(100, limit * INNER_LIMIT_MULT)
        inner_limit = min(inner_limit, INNER_LIMIT_MAX)

        # Use FTS5 for fast full-text search with BM25 ranking
        query = """
            SELECT id, name, sort_name, bm25(artists_fts) as rank
            FROM artists_fts
            WHERE artists_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """
        logger.info(f"Executing query: {query} with params: ({q}, {inner_limit})")
        cursor.execute(query, (q, inner_limit))
        raw_results = cursor.fetchall()
        logger.info(f"SQLite returned {len(raw_results)} results: {raw_results}")

        results = []
        processed_artist_ids = set()  # Track which artists we've already processed

        # Process FTS5 results
        was_cancelled_during_processing = False
        for row in raw_results:
            if token.cancelled:
                logger.info("Processing cancelled due to newer request; stopping early")
                METRICS['cancelled_during_processing'] += 1
                was_cancelled_during_processing = True
                break
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
                processed_artist_ids.add(artist_id)
                logger.info(f"Added result for {artist_id} with score {score} (BM25: {bm25_rank}, boost: {boost})")
            else:
                logger.warning(f"Could not load data for artist {artist_id}")

        # If we got very few results, try fuzzy search as fallback (guard short queries)
        if len(results) < 20 and len(q.strip()) >= FUZZY_MIN_LEN and not token.cancelled:
            logger.info(f"Few results ({len(results)}), attempting fuzzy search fallback")
            METRICS['fuzzy_invocations'] += 1
            fuzzy_hits = fuzzy_search_artists(cursor, q)
            for artist_id, name, similarity in fuzzy_hits:
                # Skip if we already processed this artist from FTS5 results
                if artist_id in processed_artist_ids:
                    continue

                logger.info(f"Processing fuzzy result: {artist_id} - {name} (similarity: {similarity:.1f}%)")

                # Load full artist data from flat files
                artist_data = load_artist_data(artist_id)
                if artist_data:
                    # Convert similarity (0-100) to score, with penalty for being fuzzy
                    # Similarity of 75% becomes score of 75, 90% becomes 90, etc.
                    # But apply a penalty since this is a fuzzy match
                    fuzzy_penalty = 20  # Adjustable penalty
                    score = max(1, int(similarity - fuzzy_penalty))

                    search_result = map_artist_for_search(artist_data, score)
                    results.append(search_result)
                    processed_artist_ids.add(artist_id)
                    logger.info(f"Added fuzzy result for {artist_id} with score {score} (similarity: {similarity:.1f}%, penalty: {fuzzy_penalty})")
                else:
                    logger.warning(f"Could not load data for fuzzy artist {artist_id}")
        elif len(results) < 20 and len(q.strip()) < FUZZY_MIN_LEN:
            METRICS['fuzzy_skipped_short'] += 1

        # Sort results by final score descending so best matches appear first
        results.sort(key=lambda r: r["score"], reverse=True)

        # Return only the requested limit
        final_results = results[:limit]

        conn.close()

        response_time = (time.time() - start_time) * 1000
        METRICS['requests_completed'] += 1
        METRICS['results_returned_total'] += len(final_results)
        METRICS['execution_ms_total'] += response_time
        logger.info(
            "Search completed: %d results in %.2fms (cache=%s, cancelled=%s, fuzzy=%s, inner_limit=%d)",
            len(final_results),
            response_time,
            'miss',
            'yes' if was_cancelled_during_processing else 'no',
            'yes' if len(results) < 20 and len(q.strip()) >= FUZZY_MIN_LEN and not token.cancelled else 'no',
            inner_limit
        )
        # Store in cache
        try:
            _cache_put(cache_key, final_results, now_ms)
        except Exception:
            pass

        return final_results  # Direct return of SearchResult array

    except Exception as e:
        logger.error(f"Error searching artists: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
    finally:
        # Clear active entry if we're still the current token
        try:
            ck = _client_key(request)
            current = _active_requests.get(ck)
            if token is not None and current and current.get('token') is token:
                _active_requests.pop(ck, None)
        except Exception:
            pass
        finally:
            # Always decrement active counter
            if METRICS['requests_active'] > 0:
                METRICS['requests_active'] -= 1


@app.get("/search/artists")
async def search_artists(
    request: Request,
    q: str = Query(..., description="Search query for artist name"),
    limit: int = Query(100, ge=1, le=100, description="Maximum number of results")
) -> List[Dict[str, Any]]:
    """Search artists and return schema-compliant SearchResult objects."""
    return await _search_artists_impl(q=q, limit=limit, request=request)


@app.get("/api/v1/search")
async def search(
    request: Request,
    type: str = Query("all", alias="type"),
    query: str = Query(..., min_length=1, alias="query")
) -> List[Dict[str, Any]]:
    """Lidarr-compatible search endpoint."""
    if type.lower() not in {"all", "artist"}:
        raise HTTPException(status_code=400, detail="Only 'artist' and 'all' search types supported")

    # For now, all searches return artist results
    return await _search_artists_impl(q=query, limit=100, request=request)


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get search service statistics."""
    try:
        stats = {
            "search_indexes": {},
            "total_size_mb": 0,
            "metrics": {
                **METRICS,
                # Derived metrics
                "avg_execution_ms": (
                    METRICS['execution_ms_total'] / METRICS['requests_completed']
                    if METRICS['requests_completed'] > 0 else 0.0
                )
            },
            "config": {
                "MIN_QUERY_LEN": MIN_QUERY_LEN,
                "DEBOUNCE_MS": DEBOUNCE_MS,
                "FUZZY_MIN_LEN": FUZZY_MIN_LEN,
                "CACHE_TTL_MS": CACHE_TTL_MS,
                "CACHE_MAX_SIZE": CACHE_MAX_SIZE,
                "INNER_LIMIT_MULT": INNER_LIMIT_MULT,
                "INNER_LIMIT_MAX": INNER_LIMIT_MAX
            }
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
