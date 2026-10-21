import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

logger = logging.getLogger(__name__)


class BlueprintLockManager:

    def __init__(
        self,
        enabled: bool = True,
        behavior: str = "skip",
    ):
        self.enabled = enabled
        self.behavior = behavior.lower()
        self._locks: dict[int, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def _get_lock(self, blueprint_id: int) -> asyncio.Lock:
        async with self._global_lock:
            if blueprint_id not in self._locks:
                self._locks[blueprint_id] = asyncio.Lock()
            return self._locks[blueprint_id]

    @asynccontextmanager
    async def lock(self, blueprint_id: int) -> AsyncGenerator[bool, None]:
        if not self.enabled:
            yield True
            return

        lock_obj = await self._get_lock(blueprint_id)

        if self.behavior == "skip":
            if lock_obj.locked():
                logger.info("Blueprint %d already running, skipping trigger", blueprint_id)
                yield False
                return

            await lock_obj.acquire()
            logger.info("Blueprint lock acquired for blueprint %d", blueprint_id)
            try:
                yield True
            finally:
                lock_obj.release()
                logger.info("Blueprint lock released for blueprint %d", blueprint_id)
        else:
            # "queue" behavior
            await lock_obj.acquire()
            logger.info("Blueprint lock acquired for blueprint %d", blueprint_id)
            try:
                yield True
            finally:
                lock_obj.release()
                logger.info("Blueprint lock released for blueprint %d", blueprint_id)
