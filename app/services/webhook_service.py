import logging

from app.infrastructure.kafka_producer import KafkaProducer
from app.schemas.github import GitHubPushEvent

logger = logging.getLogger(__name__)


class WebhookService:

    def __init__(
        self,
        kafka_producer: KafkaProducer,
    ):
        self.kafka_producer = kafka_producer

    async def process(self, event: GitHubPushEvent):

        repository = (
            f"{event.repository.owner.login}/"
            f"{event.repository.name}"
        )

        logger.info(
            "Received webhook for repository: %s",
            repository,
        )

        await self.kafka_producer.send_repository_event(repository)