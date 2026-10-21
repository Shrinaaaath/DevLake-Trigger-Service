import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class InMemoryEventDeduplicator:

    def __init__(
        self,
        window_seconds: int = 30,
        enabled: bool = True,
    ):
        self.window_seconds = window_seconds
        self.enabled = enabled
        self._seen_events: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def should_process(self, repository: str) -> bool:
        if not self.enabled:
            logger.info("Deduplication disabled: Event accepted for repository: %s", repository)
            return True

        async with self._lock:
            now = time.monotonic()
            self._purge_expired(now)

            last_seen = self._seen_events.get(repository)
            if last_seen is not None and (now - last_seen) < self.window_seconds:
                logger.info(
                    "Duplicate event skipped for repository: %s (last processed %.1fs ago, window=%ds)",
                    repository,
                    now - last_seen,
                    self.window_seconds,
                )
                return False

            self._seen_events[repository] = now
            logger.info("Event accepted for repository: %s", repository)
            return True

    def _purge_expired(self, now: float) -> None:
        expired_keys = [
            repo
            for repo, timestamp in self._seen_events.items()
            if (now - timestamp) >= self.window_seconds
        ]
        for repo in expired_keys:
            del self._seen_events[repo]
