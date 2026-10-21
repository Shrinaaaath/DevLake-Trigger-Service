from app.models.collection_job import CollectionJob
from app.models.execution_plan import ExecutionPlan
from app.services.blueprint_resolver import BlueprintResolver
import logging

logger = logging.getLogger(__name__)

class CollectionPlanner:

    def __init__(
        self,
        blueprint_resolver: BlueprintResolver,
    ):
        self.blueprint_resolver = blueprint_resolver

    async def create_plan(
        self,
        job: CollectionJob,
    ) -> ExecutionPlan:

        blueprint_ids = await self.blueprint_resolver.resolve(
            job.repositories
        )

        logger.info(
            "Planner resolved blueprints: %s",
            blueprint_ids,
        )

        return ExecutionPlan(
            blueprint_ids=blueprint_ids
        )