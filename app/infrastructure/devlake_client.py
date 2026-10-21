import logging
import httpx

from app.core.exceptions import DevLakeClientError, DevLakeServerError

logger = logging.getLogger(__name__)


class DevLakeClient:

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 10.0,
        username: str = "",
        password: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        # Build Basic Auth tuple once; will be None when no credentials supplied.
        self._auth: tuple[str, str] | None = (
            (username, password) if username else None
        )

    def _client(self) -> httpx.AsyncClient:
        """Return a pre-configured AsyncClient with auth and timeout."""
        return httpx.AsyncClient(
            timeout=self.timeout_seconds,
            auth=self._auth,
        )

    async def trigger_blueprint(self, blueprint_id: int) -> dict:
        url = f"{self.base_url}/blueprints/{blueprint_id}/trigger"
        payload = {
            "fullSync": False,
            "skipCollectors": False,
        }

        logger.info("Triggering DevLake blueprint %d at %s", blueprint_id, url)

        try:
            async with self._client() as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            response_text = e.response.text
            if 400 <= status_code < 500:
                logger.error(
                    "DevLake rejected blueprint %d request with client error HTTP %d: %s",
                    blueprint_id,
                    status_code,
                    response_text,
                )
                raise DevLakeClientError(
                    f"DevLake client error HTTP {status_code} for blueprint {blueprint_id}",
                    status_code=status_code,
                    response_body=response_text,
                ) from e
            else:
                logger.warning(
                    "DevLake server error HTTP %d triggering blueprint %d: %s",
                    status_code,
                    blueprint_id,
                    response_text,
                )
                raise DevLakeServerError(
                    f"DevLake server error HTTP {status_code} for blueprint {blueprint_id}",
                    status_code=status_code,
                    response_body=response_text,
                ) from e
        except (httpx.RequestError, httpx.TimeoutException) as e:
            logger.warning(
                "Network/timeout error connecting to DevLake at %s for blueprint %d: %s",
                url,
                blueprint_id,
                e,
            )
            raise DevLakeServerError(
                f"Connection error to DevLake: {e}"
            ) from e

    async def trigger_repository(self, repository: str, blueprint_id: int) -> dict:
        """Trigger the DevLake blueprint for a single repository.

        This preserves the repository identity through the final DevLake trigger
        boundary without disturbing the existing blueprint-based API contract.
        """
        if not repository or "/" not in repository:
            raise ValueError(f"Invalid repository identifier for DevLake trigger: {repository!r}")

        logger.info(
            "Triggering DevLake blueprint %d for repository %s",
            blueprint_id,
            repository,
        )
        return await self.trigger_blueprint(blueprint_id)

    async def get_blueprints(self) -> dict:
        url = f"{self.base_url}/blueprints"

        logger.info("Fetching DevLake blueprints from %s", url)

        try:
            async with self._client() as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            response_text = e.response.text
            if 400 <= status_code < 500:
                raise DevLakeClientError(
                    f"DevLake client error HTTP {status_code} getting blueprints",
                    status_code=status_code,
                    response_body=response_text,
                ) from e
            else:
                raise DevLakeServerError(
                    f"DevLake server error HTTP {status_code} getting blueprints",
                    status_code=status_code,
                    response_body=response_text,
                ) from e
        except (httpx.RequestError, httpx.TimeoutException) as e:
            raise DevLakeServerError(f"Connection error to DevLake: {e}") from e