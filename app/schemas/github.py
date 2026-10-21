from pydantic import BaseModel


class Owner(BaseModel):
    login: str


class Repository(BaseModel):
    name: str
    owner: Owner


class GitHubPushEvent(BaseModel):
    repository: Repository