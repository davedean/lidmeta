#!/usr/bin/env python3
"""
Test script to validate SQLite integration with flat file data.
"""

import asyncio
import sys
import os

# Add current directory to path for imports
sys.path.append('.')
from main import search_artists, load_artist_data

async def test_sqlite_integration():
    """Test the complete SQLite + flat file integration."""
    print("🔍 Testing SQLite Integration...")
    print("=" * 50)

    # Test 1: Search for artist with known flat file
    print("\n1️⃣ Testing search for 'Petkeviča'...")
    results = await search_artists(q='Petkeviča', limit=1)

    if results:
        print(f"✅ Found {len(results)} result(s)")
        result = results[0]
        artist = result['artist']
        print(f"   Artist: {artist['artistname']}")
        print(f"   MBID: {artist['id']}")
        print(f"   Score: {result['score']}")
        print(f"   Country: {artist.get('country', 'N/A')}")
        print(f"   Type: {artist.get('type', 'N/A')}")
    else:
        print("❌ No results found")
        return False

    # Test 2: Test flat file loading directly
    print("\n2️⃣ Testing direct flat file loading...")
    test_mbid = "000002a0-8f8a-4320-ac61-7f60e8b44f32"
    artist_data = load_artist_data(test_mbid)

    if artist_data:
        print(f"✅ Successfully loaded artist data")
        print(f"   Artist: {artist_data['artistname']}")
        print(f"   Keys: {list(artist_data.keys())[:10]}...")
    else:
        print("❌ Failed to load artist data")
        return False

    # Test 3: Search for common term
    print("\n3️⃣ Testing search for common term 'the'...")
    results = await search_artists(q='the', limit=3)
    print(f"✅ Found {len(results)} result(s)")

    for i, result in enumerate(results[:3], 1):
        artist = result['artist']
        print(f"   {i}. {artist['artistname']} (Score: {result['score']})")

    print("\n🎉 All tests passed! SQLite integration is working correctly.")
    return True

if __name__ == "__main__":
    print("SQLite + Flat File Integration Test")
    print("==================================")

    # Check if database exists
    db_path = "../deploy/data/processed/artist.db"
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        sys.exit(1)

    # Run tests
    success = asyncio.run(test_sqlite_integration())

    if success:
        print("\n✅ Integration test PASSED!")
        sys.exit(0)
    else:
        print("\n❌ Integration test FAILED!")
        sys.exit(1)
