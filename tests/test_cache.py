"""Tests for cache implementations."""

import time

import pytest

from lidarr_metadata_server.core.cache import (
    MemoryCache,
    create_cache,
    get_default_cache,
)


class TestMemoryCache:
    """Test the memory cache implementation."""

    @pytest.fixture
    def cache(self):
        """Create a fresh cache instance for each test."""
        return MemoryCache(max_size=10)

    @pytest.mark.asyncio
    async def test_set_and_get(self, cache):
        """Test basic set and get operations."""
        await cache.set("test_key", "test_value", 60)
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
    async def test_ttl_expiration(self, cache):
        """Test that TTL expiration works."""
        await cache.set("test_key", "test_value", 0.1)  # 100ms TTL

        # Should exist immediately
        assert await cache.exists("test_key")

        # Wait for expiration
        time.sleep(0.2)

        # Should not exist after expiration
        assert not await cache.exists("test_key")
        assert await cache.get_without_fetch("test_key") is None

    @pytest.mark.asyncio
    async def test_delete(self, cache):
        """Test delete operation."""
        await cache.set("test_key", "test_value", 60)
        assert await cache.exists("test_key")

        await cache.delete("test_key")
        assert not await cache.exists("test_key")

    @pytest.mark.asyncio
    async def test_clear(self, cache):
        """Test clear operation."""
        await cache.set("key1", "value1", 60)
        await cache.set("key2", "value2", 60)

        assert await cache.exists("key1")
        assert await cache.exists("key2")

        await cache.clear()

        assert not await cache.exists("key1")
        assert not await cache.exists("key2")

    @pytest.mark.asyncio
    async def test_max_size_eviction(self, cache):
        """Test that max size eviction works."""
        # Fill cache to max size
        for i in range(10):
            await cache.set(f"key{i}", f"value{i}", 60)

        # Add one more - should evict oldest
        await cache.set("new_key", "new_value", 60)

        # Oldest key should be gone
        assert not await cache.exists("key0")
        # New key should exist
        assert await cache.exists("new_key")

    def test_get_stats(self, cache):
        """Test cache statistics."""
        stats = cache.get_stats()

        assert "total_entries" in stats
        assert "active_entries" in stats
        assert "expired_entries" in stats
        assert "max_size" in stats
        assert "utilization" in stats

        assert stats["max_size"] == 10
        assert stats["total_entries"] == 0
        assert stats["active_entries"] == 0


class TestCacheFactory:
    """Test the cache factory functions."""

    def test_create_cache_memory(self):
        """Test creating memory cache."""
        cache = create_cache(backend="memory", max_size=500)
        assert isinstance(cache, MemoryCache)
        assert cache.max_size == 500

    def test_create_cache_default(self):
        """Test creating cache with default settings."""
        cache = create_cache()
        assert isinstance(cache, MemoryCache)

    def test_create_cache_invalid_backend(self):
        """Test that invalid backend raises error."""
        with pytest.raises(ValueError, match="Unsupported cache backend"):
            create_cache(backend="invalid")

    def test_create_cache_sqlite_implemented(self):
        """Test that SQLite backend is now implemented."""
        from lidarr_metadata_server.core.cache.sqlite import SQLiteCache
        cache = create_cache(backend="sqlite", db_path=":memory:")
        assert isinstance(cache, SQLiteCache)

    def test_create_cache_layered_not_implemented(self):
        """Test that layered backend raises NotImplementedError."""
        with pytest.raises(
            NotImplementedError, match="Layered cache not implemented yet"
        ):
            create_cache(backend="layered")

    def test_get_default_cache(self):
        """Test getting default cache."""
        cache = get_default_cache()
        assert isinstance(cache, MemoryCache)
