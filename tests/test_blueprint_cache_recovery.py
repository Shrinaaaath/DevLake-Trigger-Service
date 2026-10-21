"""Tests for cache-initialization recovery logic in StaticBlueprintResolver."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from app.infrastructure.static_blueprint_resolver import StaticBlueprintResolver
from app.core.exceptions import DevLakeServerError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cache(initialized: bool, mapping: dict[str, set[int]] | None = None):
    """Return a mock BlueprintCache."""
    cache = MagicMock()
    cache.is_initialized = initialized
    cache.get_blueprints = lambda repo: (mapping or {}).get(repo, set()).copy()
    return cache


def _make_synchronizer(raises: Exception | None = None):
    """Return a mock DevLakeSynchronizer."""
    sync = MagicMock()
    if raises:
        sync.synchronize = AsyncMock(side_effect=raises)
    else:
        sync.synchronize = AsyncMock()
    return sync


# ---------------------------------------------------------------------------
# Cache initialized — normal path (no recovery)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolve_uses_cache_when_initialized():
    cache = _make_cache(initialized=True, mapping={"owner/repo": {42}})
    resolver = StaticBlueprintResolver(cache=cache)

    result = await resolver.resolve(["owner/repo"])

    assert result == [42]


@pytest.mark.asyncio
async def test_resolve_returns_empty_list_when_repo_not_in_cache():
    cache = _make_cache(initialized=True, mapping={})
    resolver = StaticBlueprintResolver(cache=cache)

    result = await resolver.resolve(["owner/unknown-repo"])

    assert result == []


# ---------------------------------------------------------------------------
# Cache NOT initialized — recovery triggered
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recovery_sync_attempted_when_cache_not_initialized():
    sync = _make_synchronizer()
    # After synchronize() is called the 'real' code sets is_initialized, so we
    # simulate that by patching the property to True after the first call.
    call_count = [0]

    def _sync_side_effect():
        call_count[0] += 1

    sync.synchronize = AsyncMock(side_effect=_sync_side_effect)

    cache = MagicMock()
    # First access → not initialized; after sync we flip it
    cache.is_initialized = False
    cache.get_blueprints = lambda repo: {7} if repo == "owner/repo" else set()

    resolver = StaticBlueprintResolver(cache=cache, synchronizer=sync)
    result = await resolver.resolve(["owner/repo"])

    sync.synchronize.assert_awaited_once()
    assert result == [7]


@pytest.mark.asyncio
async def test_recovery_sync_succeeds_and_repo_found():
    sync = _make_synchronizer()
    cache = _make_cache(initialized=False, mapping={"owner/repo": {3, 5}})
    resolver = StaticBlueprintResolver(cache=cache, synchronizer=sync)

    result = await resolver.resolve(["owner/repo"])

    sync.synchronize.assert_awaited_once()
    assert result == [3, 5]


@pytest.mark.asyncio
async def test_recovery_sync_succeeds_but_repo_not_found_returns_empty():
    sync = _make_synchronizer()
    # Cache has no entry for the repo even after sync
    cache = _make_cache(initialized=False, mapping={})
    resolver = StaticBlueprintResolver(cache=cache, synchronizer=sync)

    result = await resolver.resolve(["owner/missing-repo"])

    sync.synchronize.assert_awaited_once()
    assert result == []


@pytest.mark.asyncio
async def test_recovery_sync_fails_returns_empty_and_does_not_crash():
    sync = _make_synchronizer(raises=DevLakeServerError("DevLake unavailable"))
    cache = _make_cache(initialized=False, mapping={"owner/repo": {1}})
    resolver = StaticBlueprintResolver(cache=cache, synchronizer=sync)

    result = await resolver.resolve(["owner/repo"])

    sync.synchronize.assert_awaited_once()
    # Must return empty — not crash — when DevLake is down
    assert result == []


@pytest.mark.asyncio
async def test_no_synchronizer_and_cache_not_initialized_returns_empty():
    cache = _make_cache(initialized=False, mapping={"owner/repo": {99}})
    resolver = StaticBlueprintResolver(cache=cache, synchronizer=None)

    result = await resolver.resolve(["owner/repo"])

    assert result == []


# ---------------------------------------------------------------------------
# Recovery not re-attempted once cache is initialized
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_recovery_when_cache_already_initialized():
    sync = _make_synchronizer()
    cache = _make_cache(initialized=True, mapping={"owner/repo": {10}})
    resolver = StaticBlueprintResolver(cache=cache, synchronizer=sync)

    result = await resolver.resolve(["owner/repo"])

    # synchronizer.synchronize should NOT be called
    sync.synchronize.assert_not_awaited()
    assert result == [10]
