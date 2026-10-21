from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.core.exceptions import ConfigurationError


class Settings(BaseSettings):

    github_webhook_secret: str
    devlake_url: str
    devlake_username: str
    devlake_password: str
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "devlake-repository-events"
    kafka_consumer_group: str = "devlake-trigger-group"

    dedup_window_seconds: int = 30
    enable_deduplication: bool = True
    enable_blueprint_locking: bool = True
    blueprint_lock_behavior: str = "skip"  # "skip" or "queue"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

    @field_validator("github_webhook_secret")
    @classmethod
    def validate_github_secret(cls, v: str) -> str:
        if not v or not v.strip():
            raise ConfigurationError("GITHUB_WEBHOOK_SECRET must not be empty")
        return v.strip()

    @field_validator("devlake_url")
    @classmethod
    def validate_devlake_url(cls, v: str) -> str:
        if not v or not v.strip():
            raise ConfigurationError("DEVLAKE_URL must not be empty")
        v_clean = v.strip()
        if not (v_clean.startswith("http://") or v_clean.startswith("https://")):
            raise ConfigurationError(f"DEVLAKE_URL must start with http:// or https://, got: '{v}'")
        return v_clean.rstrip("/")

    @field_validator("kafka_bootstrap_servers", "kafka_topic", "kafka_consumer_group")
    @classmethod
    def validate_non_empty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ConfigurationError(f"{info.field_name.upper()} must not be empty")
        return v.strip()

    @field_validator("blueprint_lock_behavior")
    @classmethod
    def validate_lock_behavior(cls, v: str) -> str:
        v_clean = v.strip().lower()
        if v_clean not in ("skip", "queue"):
            raise ConfigurationError(f"BLUEPRINT_LOCK_BEHAVIOR must be 'skip' or 'queue', got: '{v}'")
        return v_clean


def load_settings() -> Settings:
    try:
        return Settings()
    except Exception as e:
        if isinstance(e, ConfigurationError):
            raise
        raise ConfigurationError(f"Failed to load application settings: {e}") from e


settings = load_settings()