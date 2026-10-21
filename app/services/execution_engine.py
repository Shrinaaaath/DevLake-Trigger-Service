from app.services.blueprint_execution_command import BlueprintExecutionCommand


class ExecutionEngine:

    def __init__(self, devlake_client):
        self.devlake_client = devlake_client

    async def execute(self, plan):

        for blueprint_id in plan.blueprint_ids:

            command = BlueprintExecutionCommand(
                blueprint_id,
                self.devlake_client,
            )

            await command.execute()