from abc import ABC, abstractmethod
import asyncio
import logging

from app.core.exceptions import PermanentError

logger = logging.getLogger(__name__)


class ExecutionCommand(ABC):

    MAX_RETRIES = 3
    INITIAL_DELAY = 1.0  # seconds

    @abstractmethod
    async def _execute(self):
        """Perform the actual command execution."""
        pass

    async def execute(self):
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                return await self._execute()
            except PermanentError as pe:
                logger.error(
                    "Command failed with non-retryable error on attempt %d/%d: %s",
                    attempt,
                    self.MAX_RETRIES,
                    pe,
                )
                raise
            except Exception as e:
                if attempt == self.MAX_RETRIES:
                    logger.error(
                        "Command failed after %d attempts: %s",
                        self.MAX_RETRIES,
                        e,
                        exc_info=True,
                    )
                    raise

                delay = self.INITIAL_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    "Attempt %d/%d failed with transient error: %s. Retrying in %.1f second(s)...",
                    attempt,
                    self.MAX_RETRIES,
                    e,
                    delay,
                )
                await asyncio.sleep(delay)