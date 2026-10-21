import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock

from app.infrastructure.in_memory_event_deduplicator import InMemoryEventDeduplicator
from app.infrastructure.blueprint_lock_manager import BlueprintLockManager


@pytest.mark.asyncio
async def test_duplicate_repository_events_skipped():
    dedup = InMemoryEventDeduplicator(window_seconds=10, enabled=True)

    first = await dedup.should_process("owner/repo-a")
    second = await dedup.should_process("owner/repo-a")

    assert first is True
    assert second is False


@pytest.mark.asyncio
async def test_events_outside_window_processed():
    dedup = InMemoryEventDeduplicator(window_seconds=1, enabled=True)

    first = await dedup.should_process("owner/repo-b")
    assert first is True

    # Sleep past the expiration window
    await asyncio.sleep(1.1)

    second = await dedup.should_process("owner/repo-b")
    assert second is True


@pytest.mark.asyncio
async def test_deduplication_disabled_flag():
    dedup = InMemoryEventDeduplicator(window_seconds=10, enabled=False)

    first = await dedup.should_process("owner/repo-c")
    second = await dedup.should_process("owner/repo-c")

    assert first is True
    assert second is True


@pytest.mark.asyncio
async def test_concurrent_blueprint_triggers_skip():
    lock_mgr = BlueprintLockManager(enabled=True, behavior="skip")

    acquired_1 = None
    acquired_2 = None

    async def task_1():
        nonlocal acquired_1
        async with lock_mgr.lock(blueprint_id=4) as ok:
            acquired_1 = ok
            await asyncio.sleep(0.2)

    async def task_2():
        nonlocal acquired_2
        await asyncio.sleep(0.05)  # ensure task_1 runs first
        async with lock_mgr.lock(blueprint_id=4) as ok:
            acquired_2 = ok

    await asyncio.gather(task_1(), task_2())

    assert acquired_1 is True
    assert acquired_2 is False


@pytest.mark.asyncio
async def test_concurrent_blueprint_triggers_queue():
    lock_mgr = BlueprintLockManager(enabled=True, behavior="queue")

    order = []

    async def task_1():
        async with lock_mgr.lock(blueprint_id=5) as ok:
            order.append("task_1_start")
            await asyncio.sleep(0.1)
            order.append("task_1_end")

    async def task_2():
        await asyncio.sleep(0.02)
        async with lock_mgr.lock(blueprint_id=5) as ok:
            order.append("task_2_start")

    await asyncio.gather(task_1(), task_2())

    assert order == ["task_1_start", "task_1_end", "task_2_start"]


@pytest.mark.asyncio
async def test_lock_release_after_failure():
    lock_mgr = BlueprintLockManager(enabled=True, behavior="skip")

    # Lock blueprint 6 and raise an exception inside
    with pytest.raises(RuntimeError):
        async with lock_mgr.lock(blueprint_id=6) as ok:
            assert ok is True
            raise RuntimeError("Trigger failure")

    # Lock should be released and re-acquirable
    async with lock_mgr.lock(blueprint_id=6) as ok:
        assert ok is True
