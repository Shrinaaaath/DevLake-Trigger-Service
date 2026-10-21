import hashlib
import hmac

from app.core.security import GitHubSignatureVerifier


def test_signature_verifier_accepts_valid_signature():
    payload = b'{"repository":{"name":"Repo-A"}}'
    secret = b"test-secret"
    sig = "sha256=" + hmac.new(secret, payload, hashlib.sha256).hexdigest()

    verifier = GitHubSignatureVerifier()
    verifier.secret = secret

    assert verifier.verify(payload, sig) is True


def test_signature_verifier_rejects_invalid_signature():
    payload = b'{"repository":{"name":"Repo-A"}}'
    verifier = GitHubSignatureVerifier()
    verifier.secret = b"test-secret"

    assert verifier.verify(payload, "sha256=invalid") is False