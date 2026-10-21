from abc import ABC, abstractmethod


class BlueprintResolver(ABC):

    @abstractmethod
    async def resolve(
        self,
        repositories: list[str],
    ) -> list[int]:
        """
        Return the blueprint IDs that should be triggered.
        """
        pass