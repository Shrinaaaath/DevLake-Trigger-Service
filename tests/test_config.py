import pytest
from app.config import Settings
from app.core.exceptions import ConfigurationError


def test_valid_config():
    s = Settings(
        github_webhook_secret="testsecret",
        devlake_url="http://localhost:8084",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="test-topic",
        kafka_consumer_group="test-group",
    )
    assert s.github_webhook_secret == "testsecret"
    assert s.devlake_url == "http://localhost:8084"


def test_empty_github_secret_raises_config_error():
    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            github_webhook_secret="  ",
            devlake_url="http://localhost:8084",
        )
    assert "GITHUB_WEBHOOK_SECRET must not be empty" in str(exc_info.value)


def test_invalid_devlake_url_scheme_raises_config_error():
    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            github_webhook_secret="secret",
            devlake_url="ftp://localhost:8084",
        )
    assert "DEVLAKE_URL must start with http:// or https://" in str(exc_info.value)
