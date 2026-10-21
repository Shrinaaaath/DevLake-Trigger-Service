import pytest
from unittest.mock import AsyncMock
from app.domain.execution_command import ExecutionCommand
from app.core.exceptions import PermanentError, TransientError


class MockCommand(ExecutionCommand):
    def __init__(self, side_effects):
        self.side_effects = side_effects
        self.call_count = 0

    async def _execute(self):
        self.call_count += 1
        effect = self.side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


@pytest.mark.asyncio
async def test_execution_command_no_retry_on_permanent_error():
    cmd = MockCommand([PermanentError("Client error 400")])
    with pytest.raises(PermanentError):
        await cmd.execute()
    assert cmd.call_count == 1


@pytest.mark.asyncio
async def test_execution_command_retries_on_transient_error():
    cmd = MockCommand([TransientError("Server error 500"), "success"])
    cmd.INITIAL_DELAY = 0.01  # fast test
    result = await cmd.execute()
    assert result == "success"
    assert cmd.call_count == 2
