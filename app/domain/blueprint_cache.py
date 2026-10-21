from abc import ABC, abstractmethod


class BlueprintCache(ABC):

    @property
    @abstractmethod
    def is_initialized(self) -> bool:
        """True once the cache has been successfully populated at least once."""
        pass

    @abstractmethod
    def get_blueprints(
        self,
        repository: str,
    ) -> set[int]:
        pass

    @abstractmethod
    def replace(
        self,
        mapping: dict[str, set[int]],
    ) -> None:
        pass