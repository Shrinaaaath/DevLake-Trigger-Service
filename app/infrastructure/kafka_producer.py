import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer

from app.core.exceptions import KafkaPublishError, KafkaSerializationError

logger = logging.getLogger(__name__)


class KafkaProducer:

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: AIOKafkaProducer | None = None
        self._is_connected = False

    @property
    def is_connected(self) -> bool:
        return self._is_connected and self.producer is not None

    def _serialize_value(self, value: dict) -> bytes:
        try:
            return json.dumps(value).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise KafkaSerializationError(f"Failed to serialize payload to JSON: {e}") from e

    async def start(self) -> None:
        logger.info("Connecting Kafka producer to bootstrap_servers=%s", self.bootstrap_servers)
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=self._serialize_value,
            )
            await self.producer.start()
            self._is_connected = True
            logger.info("Kafka producer connected successfully to topic=%s", self.topic)
        except Exception as e:
            self._is_connected = False
            logger.error("Failed to connect Kafka producer to %s: %s", self.bootstrap_servers, e)
            raise KafkaPublishError(f"Failed to start Kafka producer: {e}") from e

    async def stop(self) -> None:
        if self.producer:
            logger.info("Stopping Kafka producer")
            try:
                await self.producer.stop()
            except Exception as e:
                logger.warning("Error stopping Kafka producer: %s", e)
            finally:
                self.producer = None
                self._is_connected = False
                logger.info("Kafka producer stopped")

    async def send_repository_event(self, repository: str, max_retries: int = 3) -> None:
        if not self.is_connected or not self.producer:
            raise KafkaPublishError("Kafka producer is not connected")

        payload = {"repository": repository}
        last_exception = None

        for attempt in range(1, max_retries + 1):
            try:
                record_metadata = await self.producer.send_and_wait(self.topic, payload)
                logger.info(
                    "Published repository event to Kafka topic=%s partition=%d offset=%d: repository=%s",
                    record_metadata.topic,
                    record_metadata.partition,
                    record_metadata.offset,
                    repository,
                )
                return
            except KafkaSerializationError:
                raise
            except Exception as e:
                last_exception = e
                logger.warning(
                    "Attempt %d/%d failed to publish repository event for %s: %s",
                    attempt,
                    max_retries,
                    repository,
                    e,
                )
                if attempt < max_retries:
                    await asyncio.sleep(0.5 * attempt)

        logger.error("All %d attempts failed to publish event for %s to Kafka topic=%s", max_retries, repository, self.topic)
        raise KafkaPublishError(
            f"Failed to publish event for {repository} after {max_retries} attempts"
        ) from last_exception
