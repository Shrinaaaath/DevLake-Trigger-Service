import asyncio
from app.domain.blueprint_cache import BlueprintCache


class InMemoryBlueprintCache(BlueprintCache):

    def __init__(self):
        self._mapping: dict[str, set[int]] = {}
        self._lock = asyncio.Lock()
        self._is_initialized = False

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    def get_blueprints(self, repository: str) -> set[int]:
        # Returns a copy of the blueprint set to prevent external mutation
        return self._mapping.get(repository, set()).copy()

    def replace(self, mapping: dict[str, set[int]]) -> None:
        # Atomic dictionary replacement
        self._mapping = {k: set(v) for k, v in mapping.items()}
        self._is_initialized = True