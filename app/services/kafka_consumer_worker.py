import asyncio
import logging
from app.core.exceptions import PermanentError
from app.infrastructure.kafka_consumer import KafkaConsumer
from app.infrastructure.in_memory_event_deduplicator import InMemoryEventDeduplicator
from app.models.collection_job import CollectionJob
from app.services.job_executor import JobExecutor

logger = logging.getLogger(__name__)


class KafkaConsumerWorker:

    def __init__(
        self,
        kafka_consumer: KafkaConsumer,
        job_executor: JobExecutor,
        event_deduplicator: InMemoryEventDeduplicator | None = None,
    ):
        self.kafka_consumer = kafka_consumer
        self.job_executor = job_executor
        self.event_deduplicator = event_deduplicator

    async def run(self) -> None:
        logger.info("Starting Kafka consumer worker loop")
        try:
            async for event in self.kafka_consumer.consume():
                try:
                    if not isinstance(event, dict):
                        logger.warning("Received invalid Kafka payload type: %s", type(event).__name__)
                        continue

                    repository = event.get("repository")
                    if not repository or not isinstance(repository, str) or "/" not in repository:
                        logger.warning("Received malformed repository event payload: %s", event)
                        continue

                    logger.info("Consumed repository event: %s", repository)

                    if self.event_deduplicator and not await self.event_deduplicator.should_process(repository):
                        continue

                    logger.info("Waiting 10 seconds before triggering DevLake...")
                    await asyncio.sleep(10)

                    job = CollectionJob(repositories=[repository])
                    await self.job_executor.execute(job)

                except PermanentError as pe:
                    logger.error("Permanent error processing repository event '%s': %s", event.get("repository"), pe)
                except Exception as e:
                    logger.error("Error processing repository event from Kafka: %s", e, exc_info=True)
        except asyncio.CancelledError:
            logger.info("Kafka consumer worker loop cancelled")
        except Exception as e:
            logger.error("Fatal error in Kafka consumer worker loop: %s", e, exc_info=True)
