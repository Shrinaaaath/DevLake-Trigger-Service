import logging

from app.domain.execution_command import ExecutionCommand

logger = logging.getLogger(__name__)


class BlueprintExecutionCommand(ExecutionCommand):

    def __init__(
        self,
        blueprint_id: int,
        devlake_client,
        repository: str | None = None,
    ):
        self.blueprint_id = blueprint_id
        self.devlake_client = devlake_client
        self.repository = repository

    async def _execute(self):

        logger.info(
            "Triggering blueprint %d for repository %s",
            self.blueprint_id,
            self.repository,
        )

        if self.repository:
            result = await self.devlake_client.trigger_repository(
                self.repository,
                self.blueprint_id,
            )
        else:
            result = await self.devlake_client.trigger_blueprint(
                self.blueprint_id
            )

        logger.info(
            "Blueprint %d triggered successfully for repository %s. Pipeline ID: %s",
            self.blueprint_id,
            self.repository,
            result["id"],
        )

        return result