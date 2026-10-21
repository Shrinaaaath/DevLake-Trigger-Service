from app.domain.blueprint_cache import BlueprintCache
from app.infrastructure.devlake_client import DevLakeClient
import logging

logger = logging.getLogger(__name__)


class DevLakeSynchronizer:

    def __init__(
        self,
        devlake_client: DevLakeClient,
        blueprint_cache: BlueprintCache,
    ):
        self.devlake_client = devlake_client
        self.blueprint_cache = blueprint_cache

    async def synchronize(self):
        logger.info("Synchronizing blueprint cache...")
        response = await self.devlake_client.get_blueprints()

        mapping: dict[str, set[int]] = {}

        for blueprint in response["blueprints"]:

            blueprint_id = blueprint["id"]

            for stage in blueprint["plan"]:

                for task in stage:

                    if task["plugin"] != "github_graphql":
                        continue

                    repository = task["options"]["fullName"]

                    logger.info(
                        "Blueprint %d contains repository: %s",
                        blueprint_id,
                        repository,
                    )

                    mapping.setdefault(
                        repository,
                        set(),
                    ).add(blueprint_id)

        self.blueprint_cache.replace(mapping)