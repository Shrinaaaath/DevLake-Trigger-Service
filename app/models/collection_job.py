from dataclasses import dataclass


@dataclass
class CollectionJob:
    repositories: list[str]