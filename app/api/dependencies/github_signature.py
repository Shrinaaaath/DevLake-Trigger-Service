from fastapi import Header, HTTPException, Request

from app.core.security import verifier


async def verify_github_signature(
    request: Request,
    x_hub_signature_256: str | None = Header(default=None),
) -> None:

    if x_hub_signature_256 is None:
        raise HTTPException(
            status_code=401,
            detail="Missing GitHub signature",
        )

    payload = await request.body()

    if not verifier.verify(payload, x_hub_signature_256):
        raise HTTPException(
            status_code=401,
            detail="Invalid GitHub signature",
        )