import pytest
from unittest.mock import patch, AsyncMock
import httpx
from app.infrastructure.devlake_client import DevLakeClient
from app.core.exceptions import DevLakeClientError, DevLakeServerError


@pytest.mark.asyncio
async def test_devlake_client_4xx_raises_client_error():
    client = DevLakeClient("http://localhost:8084")

    mock_response = httpx.Response(status_code=404, json={"message": "Blueprint not found"}, request=httpx.Request("POST", "http://localhost:8084/blueprints/99/trigger"))

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(DevLakeClientError) as exc_info:
            await client.trigger_blueprint(99)

        assert exc_info.value.status_code == 404
        assert "404" in str(exc_info.value)


@pytest.mark.asyncio
async def test_devlake_client_5xx_raises_server_error():
    client = DevLakeClient("http://localhost:8084")

    mock_response = httpx.Response(status_code=500, json={"message": "Internal error"}, request=httpx.Request("POST", "http://localhost:8084/blueprints/1/trigger"))

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response

        with pytest.raises(DevLakeServerError) as exc_info:
            await client.trigger_blueprint(1)

        assert exc_info.value.status_code == 500
        assert "500" in str(exc_info.value)


@pytest.mark.asyncio
async def test_devlake_client_trigger_repository_uses_blueprint_call_for_repo_scope():
    client = DevLakeClient("http://localhost:8084")

    with patch.object(client, "trigger_blueprint", new_callable=AsyncMock) as mock_trigger:
        mock_trigger.return_value = {"id": "pipeline-42"}

        result = await client.trigger_repository("acme/demo-repo", 7)

        mock_trigger.assert_awaited_once_with(7)
        assert result == {"id": "pipeline-42"}
