import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest
from fastapi.testclient import TestClient

from app.schemas.github import GitHubPushEvent, Repository, Owner
from app.services.webhook_service import WebhookService
from app.services.kafka_consumer_worker import KafkaConsumerWorker
from app.models.collection_job import CollectionJob
from app.main import app


@pytest.mark.asyncio
async def test_webhook_service_publishes_to_kafka():
    mock_producer = AsyncMock()
    webhook_service = WebhookService(kafka_producer=mock_producer)

    event = GitHubPushEvent(
        repository=Repository(
            name="test-repo",
            owner=Owner(login="test-owner")
        )
    )

    await webhook_service.process(event)

    mock_producer.send_repository_event.assert_awaited_once_with("test-owner/test-repo")


@pytest.mark.asyncio
async def test_kafka_consumer_worker_executes_job():
    mock_consumer = MagicMock()
    mock_executor = AsyncMock()

    async def sample_messages():
        yield {"repository": "owner/repo-1"}
        yield {"repository": "owner/repo-2"}

    mock_consumer.consume = sample_messages

    worker = KafkaConsumerWorker(kafka_consumer=mock_consumer, job_executor=mock_executor)
    await worker.run()

    assert mock_executor.execute.await_count == 2
    mock_executor.execute.assert_any_await(CollectionJob(repositories=["owner/repo-1"]))
    mock_executor.execute.assert_any_await(CollectionJob(repositories=["owner/repo-2"]))


@pytest.mark.asyncio
async def test_kafka_consumer_worker_skips_malformed_events():
    mock_consumer = MagicMock()
    mock_executor = AsyncMock()

    async def sample_messages():
        yield "not-a-dict"
        yield {"repository": 12345}  # non-string
        yield {"repository": "invalid-no-slash"}  # invalid format
        yield {"repository": "valid-owner/valid-repo"}

    mock_consumer.consume = sample_messages

    worker = KafkaConsumerWorker(kafka_consumer=mock_consumer, job_executor=mock_executor)
    await worker.run()

    # Only valid repository should be executed
    assert mock_executor.execute.await_count == 1
    mock_executor.execute.assert_called_once_with(CollectionJob(repositories=["valid-owner/valid-repo"]))


@pytest.mark.asyncio
async def test_kafka_consumer_worker_handles_exceptions_gracefully():
    mock_consumer = MagicMock()
    mock_executor = AsyncMock()
    # First job raises exception, second job succeeds
    mock_executor.execute.side_effect = [Exception("DevLake API error"), None]

    async def sample_messages():
        yield {"repository": "owner/failing-repo"}
        yield {"repository": "owner/success-repo"}

    mock_consumer.consume = sample_messages

    worker = KafkaConsumerWorker(kafka_consumer=mock_consumer, job_executor=mock_executor)
    # Should not raise exception
    await worker.run()

    assert mock_executor.execute.await_count == 2


def test_health_check_endpoint():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
