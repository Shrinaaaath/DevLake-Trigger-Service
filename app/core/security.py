import hashlib
import hmac

from app.config import settings


class GitHubSignatureVerifier:

    def __init__(self):
        self.secret = settings.github_webhook_secret.encode()

    def verify(self, payload: bytes, signature: str) -> bool:
        digest = hmac.new(
            self.secret,
            payload,
            hashlib.sha256,
        ).hexdigest()

        expected = f"sha256={digest}"

        return hmac.compare_digest(expected, signature)


verifier = GitHubSignatureVerifier()