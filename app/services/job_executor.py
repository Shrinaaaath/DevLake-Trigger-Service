import logging
from app.infrastructure.devlake_client import DevLakeClient
from app.infrastructure.blueprint_lock_manager import BlueprintLockManager
from app.models.collection_job import CollectionJob
from app.services.collection_planner import CollectionPlanner
from app.services.blueprint_execution_command import (
    BlueprintExecutionCommand,
)

logger = logging.getLogger(__name__)


class JobExecutor:

    def __init__(
        self,
        devlake_client: DevLakeClient,
        collection_planner: CollectionPlanner,
        blueprint_lock_manager: BlueprintLockManager | None = None,
    ):
        self.devlake_client = devlake_client
        self.collection_planner = collection_planner
        self.blueprint_lock_manager = blueprint_lock_manager

    async def execute(
        self,
        job: CollectionJob,
    ):
        logger.info("Executing collection job")

        for repository in job.repositories:
            logger.info("  %s", repository)

            repo_job = CollectionJob(repositories=[repository])
            plan = await self.collection_planner.create_plan(repo_job)

            logger.info(
                "Resolved blueprint IDs for repository %s: %s",
                repository,
                plan.blueprint_ids,
            )

            for blueprint in plan.blueprint_ids:
                if self.blueprint_lock_manager:
                    async with self.blueprint_lock_manager.lock(blueprint) as acquired:
                        if not acquired:
                            continue
                        command = BlueprintExecutionCommand(
                            blueprint,
                            self.devlake_client,
                            repository=repository,
                        )
                        await command.execute()
                else:
                    command = BlueprintExecutionCommand(
                        blueprint,
                        self.devlake_client,
                        repository=repository,
                    )
                    await command.execute()