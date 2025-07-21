"""Tests for SQLite cache implementation."""

import asyncio
import json
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from lidarr_metadata_server.core.cache import SQLiteCache


@pytest_asyncio.fixture
async def cache():
    """Create a fresh cache instance for each test."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    cache = SQLiteCache(db_path)
    try:
        yield cache
    finally:
        await cache.close()
        Path(db_path).unlink(missing_ok=True)


class TestSQLiteCache:
    """Test the SQLite cache implementation."""

    @pytest.mark.asyncio
    async def test_set_and_get(self, cache):
        """Test basic set and get operations."""
        await cache.set("test_key", "test_value", None)
        result = await cache.get_without_fetch("test_key")
        assert result == "test_value"

    @pytest.mark.asyncio
    async def test_get_with_fetch(self, cache):
        """Test get with fetch function."""

        def fetch_fn():
            return "fetched_value"

        result = await cache.get("test_key", 60, fetch_fn)
        assert result == "fetched_value"

        # Second call should use cache
        result2 = await cache.get("test_key", 60, fetch_fn)
        assert result2 == "fetched_value"

    @pytest.mark.asyncio
    async def test_get_with_async_fetch(self, cache):
        """Test get with async fetch function."""

        async def fetch_fn():
            return "async_fetched_value"

        result = await cache.get("test_key", 60, fetch_fn)
        assert result == "async_fetched_value"

    @pytest.mark.asyncio
    async def test_stale_data_returned(self, cache):
        """Test that stale data is returned immediately."""
        await cache.set("test_key", "old_value", None)

        # Should return stale data immediately
        result = await cache.get_without_fetch("test_key")
        assert result == "old_value"

        # Should still exist even after TTL
        await asyncio.sleep(0.2)
        result = await cache.get_without_fetch("test_key")
        assert result == "old_value"

    @pytest.mark.asyncio
    async def test_background_refresh_scheduled(self, cache):
        """Test that background refresh is scheduled for expired data."""
        refresh_called = False

        async def fetch_fn():
            nonlocal refresh_called
            refresh_called = True
            return "new_value"

        # Set data
        await cache.set("test_key", "old_value", None)

        # Wait for expiration
        await asyncio.sleep(0.2)

        # Get data - should return stale data and schedule refresh
        result = await cache.get("test_key", 60, fetch_fn)
        assert result == "old_value"

        # Wait for background refresh
        await asyncio.sleep(0.1)

        # Check that refresh was called
        assert refresh_called

        # Get data again - should now have new value
        result = await cache.get_without_fetch("test_key")
        assert result == "new_value"

    @pytest.mark.asyncio
    async def test_delete(self, cache):
        """Test delete operation."""
        await cache.set("test_key", "test_value", None)
        assert await cache.exists("test_key")

        await cache.delete("test_key")
        assert not await cache.exists("test_key")

    @pytest.mark.asyncio
    async def test_clear(self, cache):
        """Test clear operation."""
        await cache.set("key1", "value1", None)
        await cache.set("key2", "value2", None)

        assert await cache.exists("key1")
        assert await cache.exists("key2")

        await cache.clear()

        assert not await cache.exists("key1")
        assert not await cache.exists("key2")

    @pytest.mark.asyncio
    async def test_complex_objects(self, cache):
        """Test caching complex objects (lists, dicts)."""
        complex_obj = {
            "string": "test",
            "number": 42,
            "list": [1, 2, 3],
            "nested": {"key": "value"},
        }

        await cache.set("complex_key", complex_obj, None)
        result = await cache.get_without_fetch("complex_key")

        assert result == complex_obj
        assert result["string"] == "test"
        assert result["list"] == [1, 2, 3]
        assert result["nested"]["key"] == "value"

    @pytest.mark.asyncio
    async def test_cleanup_expired(self, cache):
        """Test cleanup of expired entries."""
        # Add some entries with different TTLs
        await cache.set("key1", "value1", None)  # Expires in 1 second
        await asyncio.sleep(1.1)  # Wait for key1 to expire
        await cache.set("key2", "value2", None)  # Stays valid
        await asyncio.sleep(0.1)  # Ensure key2 is set after key1 expired

        # Cleanup should remove only key1
        removed_count = await cache.cleanup_expired()
        assert removed_count == 1

        # Check that only valid entry remains
        assert not await cache.exists("key1")
        assert await cache.exists("key2")

    @pytest.mark.asyncio
    async def test_get_stats(self, cache):
        """Test cache statistics."""
        # Add some entries
        await cache.set("key1", "value1", None)
        await cache.set("key2", "value2", None)

        stats = await cache.get_stats()

        assert "total_entries" in stats
        assert "active_entries" in stats
        assert "expired_entries" in stats
        assert "database_size_bytes" in stats
        assert "database_size_mb" in stats
        assert "refresh_queue_size" in stats

        assert stats["total_entries"] == 2
        assert stats["active_entries"] == 2
        assert stats["expired_entries"] == 0
        assert stats["database_size_bytes"] > 0
        assert stats["refresh_queue_size"] == 0

    @pytest.mark.asyncio
    async def test_persistence(self, cache):
        """Test that data persists across cache instances."""
        # Add data to first cache instance
        await cache.set("persistent_key", "persistent_value", None)

        # Close first cache
        await cache.close()

        # Create new cache instance with same database
        new_cache = SQLiteCache(cache.db_path)

        # Data should still be there
        result = await new_cache.get_without_fetch("persistent_key")
        assert result == "persistent_value"

        await new_cache.close()

    @pytest.mark.asyncio
    async def test_concurrent_access(self, cache):
        """Test concurrent access to the cache."""

        async def set_value(key: str, value: str):
            await cache.set(key, value, None)

        async def get_value(key: str):
            return await cache.get_without_fetch(key)

        # Run concurrent operations
        tasks = [set_value(f"key{i}", f"value{i}") for i in range(10)]
        await asyncio.gather(*tasks)

        # Verify all values were set correctly
        for i in range(10):
            result = await get_value(f"key{i}")
            assert result == f"value{i}"


class TestSQLiteCacheFactory:
    """Test SQLite cache creation via factory."""

    def test_create_sqlite_cache(self):
        """Test creating SQLite cache via factory."""
        from lidarr_metadata_server.core.cache import create_cache

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            cache = create_cache(backend="sqlite", db_path=db_path)
            assert isinstance(cache, SQLiteCache)
            assert cache.db_path == Path(db_path)
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_create_sqlite_cache_with_env(self, monkeypatch):
        """Test creating SQLite cache with environment variables."""
        from lidarr_metadata_server.core.cache import create_cache

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            monkeypatch.setenv("LMS_CACHE_BACKEND", "sqlite")
            monkeypatch.setenv("LMS_SQLITE_PATH", db_path)

            cache = create_cache()
            assert isinstance(cache, SQLiteCache)
            assert cache.db_path == Path(db_path)
        finally:
            Path(db_path).unlink(missing_ok=True)
