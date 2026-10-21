import json
import logging
from typing import AsyncGenerator, Dict, Any
from aiokafka import AIOKafkaConsumer

from app.core.exceptions import KafkaConsumeError, KafkaSerializationError

logger = logging.getLogger(__name__)


def _safe_deserialize(payload_bytes: bytes) -> Dict[str, Any]:
    try:
        data = json.loads(payload_bytes.decode("utf-8"))
        if not isinstance(data, dict):
            raise KafkaSerializationError(f"Expected JSON object message, got {type(data).__name__}")
        return data
    except Exception as e:
        if isinstance(e, KafkaSerializationError):
            raise
        raise KafkaSerializationError(f"Failed to deserialize Kafka message payload: {e}") from e


class KafkaConsumer:

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer: AIOKafkaConsumer | None = None
        self._is_connected = False

    @property
    def is_connected(self) -> bool:
        return self._is_connected and self.consumer is not None

    async def start(self) -> None:
        logger.info(
            "Connecting Kafka consumer to bootstrap_servers=%s, topic=%s, group_id=%s",
            self.bootstrap_servers,
            self.topic,
            self.group_id,
        )
        try:
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=_safe_deserialize,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            await self.consumer.start()
            self._is_connected = True
            logger.info("Kafka consumer connected successfully")
        except Exception as e:
            self._is_connected = False
            logger.error("Failed to start Kafka consumer: %s", e)
            raise KafkaConsumeError(f"Failed to start Kafka consumer: {e}") from e

    async def stop(self) -> None:
        if self.consumer:
            logger.info("Stopping Kafka consumer")
            try:
                await self.consumer.stop()
            except Exception as e:
                logger.warning("Error stopping Kafka consumer: %s", e)
            finally:
                self.consumer = None
                self._is_connected = False
                logger.info("Kafka consumer stopped")

    async def consume(self) -> AsyncGenerator[Dict[str, Any], None]:
        if not self.is_connected or not self.consumer:
            raise KafkaConsumeError("Kafka consumer is not connected")

        async for msg in self.consumer:
            logger.info(
                "Consumed message from topic=%s partition=%d offset=%d",
                msg.topic,
                msg.partition,
                msg.offset,
            )
            yield msg.value
