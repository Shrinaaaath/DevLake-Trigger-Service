from fastapi import APIRouter, Depends, Request
from app.api.dependencies.github_signature import verify_github_signature

from app.schemas.github import GitHubPushEvent

router = APIRouter()


@router.post("/webhook")
async def github_webhook(
    payload: GitHubPushEvent,
    request: Request,
    _: None = Depends(verify_github_signature),
):

    await request.app.state.webhook_service.process(payload)

    return {
        "status": "received"
    }