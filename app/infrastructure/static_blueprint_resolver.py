import logging
from app.domain.blueprint_cache import BlueprintCache
from app.services.blueprint_resolver import BlueprintResolver
from app.services.devlake_synchronizer import DevLakeSynchronizer

logger = logging.getLogger(__name__)


class StaticBlueprintResolver(BlueprintResolver):
    """Resolves repositories to blueprint IDs using a local cache.

    If the cache has never been successfully initialized (e.g. DevLake was
    unavailable at startup), a one-off recovery synchronization is attempted
    before the lookup is retried.  Subsequent calls use the normal scheduled
    synchronization path; this recovery is only triggered when
    ``cache.is_initialized`` is False.
    """

    def __init__(
        self,
        cache: BlueprintCache,
        synchronizer: DevLakeSynchronizer | None = None,
    ):
        self.cache = cache
        self.synchronizer = synchronizer

    async def resolve(
        self,
        repositories: list[str],
    ) -> list[int]:
        if not self.cache.is_initialized:
            recovery_succeeded = await self._attempt_recovery_sync()
            if recovery_succeeded:
                logger.info("Retrying repository lookup after recovery synchronization")
                result = self._lookup(repositories)
                if not result:
                    logger.warning(
                        "Repository still not found after successful recovery synchronization: %s",
                        repositories,
                    )
                return result
            # Recovery failed — return empty so no blueprint is triggered.
            return []

        return self._lookup(repositories)

    def _lookup(self, repositories: list[str]) -> list[int]:
        blueprints: set[int] = set()
        for repository in repositories:
            resolved = self.cache.get_blueprints(repository)
            logger.info("Lookup '%s' -> %s", repository, resolved)
            blueprints.update(resolved)
        return sorted(blueprints)

    async def _attempt_recovery_sync(self) -> bool:
        """Try to synchronize the blueprint cache from DevLake.

        Returns True if synchronization succeeded, False otherwise.
        """
        if self.synchronizer is None:
            logger.warning(
                "Blueprint cache not initialized and no synchronizer configured — "
                "cannot perform recovery synchronization"
            )
            return False

        logger.warning(
            "Blueprint cache not initialized; "
            "attempting recovery synchronization from DevLake"
        )
        try:
            await self.synchronizer.synchronize()
            logger.info("Recovery synchronization succeeded; blueprint cache is now initialized")
            return True
        except Exception as e:
            logger.error(
                "Recovery synchronization failed — DevLake may still be unavailable: %s",
                e,
            )
            return False