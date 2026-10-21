import asyncio
import logging

from app.services.devlake_synchronizer import DevLakeSynchronizer

logger = logging.getLogger(__name__)


class SynchronizationWorker:

    def __init__(
        self,
        synchronizer: DevLakeSynchronizer,
        interval_seconds: int = 300,
    ):
        self.synchronizer = synchronizer
        self.interval_seconds = interval_seconds

    async def run(self):
        logger.info("Starting synchronization worker with interval=%ds", self.interval_seconds)
        try:
            while True:
                await asyncio.sleep(self.interval_seconds)
                logger.info("Refreshing DevLake blueprint cache...")
                try:
                    await self.synchronizer.synchronize()
                    logger.info("DevLake blueprint cache refreshed successfully")
                except Exception as e:
                    logger.error("DevLake blueprint cache refresh failed: %s", e, exc_info=True)
        except asyncio.CancelledError:
            logger.info("Synchronization worker loop cancelled")