"""Custom exception definitions for the DevLake Trigger Service."""

class DevLakeTriggerError(Exception):
    """Base exception for all application errors."""
    pass


class ConfigurationError(DevLakeTriggerError):
    """Raised when application configuration validation fails at startup."""
    pass


class PermanentError(DevLakeTriggerError):
    """Base class for non-retryable errors (e.g. HTTP 4xx, invalid schema)."""
    pass


class TransientError(DevLakeTriggerError):
    """Base class for retryable errors (e.g. HTTP 5xx, network timeouts)."""
    pass


class DevLakeAPIError(DevLakeTriggerError):
    """Base exception for DevLake client API interactions."""

    def __init__(self, message: str, status_code: int | None = None, response_body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class DevLakeClientError(DevLakeAPIError, PermanentError):
    """Raised for non-retryable 4xx client errors returned by DevLake."""
    pass


class DevLakeServerError(DevLakeAPIError, TransientError):
    """Raised for retryable 5xx server errors or connection failures from DevLake."""
    pass


class KafkaMessagingError(DevLakeTriggerError):
    """Base exception for Kafka messaging operations."""
    pass


class KafkaSerializationError(KafkaMessagingError, PermanentError):
    """Raised when message serialization or deserialization fails."""
    pass


class KafkaPublishError(KafkaMessagingError, TransientError):
    """Raised when message publishing fails after transient retries."""
    pass


class KafkaConsumeError(KafkaMessagingError, TransientError):
    """Raised when message consumption encounters a transient failure."""
    pass
