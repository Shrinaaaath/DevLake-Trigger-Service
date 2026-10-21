import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.webhook import router as webhook_router
from app.api.health import router as health_router
from app.config import settings
from app.infrastructure.devlake_client import DevLakeClient
from app.infrastructure.in_memory_blueprint_cache import (
    InMemoryBlueprintCache,
)
from app.infrastructure.kafka_producer import KafkaProducer
from app.infrastructure.kafka_consumer import KafkaConsumer
from app.infrastructure.in_memory_event_deduplicator import (
    InMemoryEventDeduplicator,
)
from app.infrastructure.blueprint_lock_manager import (
    BlueprintLockManager,
)
from app.infrastructure.static_blueprint_resolver import (
    StaticBlueprintResolver,
)
from app.services.collection_planner import CollectionPlanner
from app.services.job_executor import JobExecutor
from app.services.webhook_service import WebhookService
from app.services.devlake_synchronizer import DevLakeSynchronizer
from app.services.synchronization_worker import SynchronizationWorker
from app.services.kafka_consumer_worker import KafkaConsumerWorker
from app.logging_config import configure_logging

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------

configure_logging()


# -----------------------------------------------------------------------------
# Infrastructure Instances
# -----------------------------------------------------------------------------

kafka_producer = KafkaProducer(
    bootstrap_servers=settings.kafka_bootstrap_servers,
    topic=settings.kafka_topic,
)

kafka_consumer = KafkaConsumer(
    bootstrap_servers=settings.kafka_bootstrap_servers,
    topic=settings.kafka_topic,
    group_id=settings.kafka_consumer_group,
)

event_deduplicator = InMemoryEventDeduplicator(
    window_seconds=settings.dedup_window_seconds,
    enabled=settings.enable_deduplication,
)

blueprint_lock_manager = BlueprintLockManager(
    enabled=settings.enable_blueprint_locking,
    behavior=settings.blueprint_lock_behavior,
)

blueprint_cache = InMemoryBlueprintCache()

devlake_client = DevLakeClient(
    base_url=settings.devlake_url,
    username=settings.devlake_username,
    password=settings.devlake_password,
)

synchronizer = DevLakeSynchronizer(
    devlake_client,
    blueprint_cache,
)

synchronization_worker = SynchronizationWorker(
    synchronizer,
)

# -----------------------------------------------------------------------------
# Service Instances
# -----------------------------------------------------------------------------

blueprint_resolver = StaticBlueprintResolver(
    blueprint_cache,
    synchronizer=synchronizer,
)

collection_planner = CollectionPlanner(
    blueprint_resolver,
)

job_executor = JobExecutor(
    devlake_client=devlake_client,
    collection_planner=collection_planner,
    blueprint_lock_manager=blueprint_lock_manager,
)

webhook_service = WebhookService(
    kafka_producer,
)

kafka_consumer_worker = KafkaConsumerWorker(
    kafka_consumer=kafka_consumer,
    job_executor=job_executor,
    event_deduplicator=event_deduplicator,
)


# -----------------------------------------------------------------------------
# Application Lifespan
# -----------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing DevLake Trigger Service startup...")

    try:
        await kafka_producer.start()
    except Exception as e:
        logger.error("Kafka producer startup failed: %s", e)

    try:
        await kafka_consumer.start()
    except Exception as e:
        logger.error("Kafka consumer startup failed: %s", e)

    try:
        await synchronizer.synchronize()
        logger.info("Initial DevLake blueprint synchronization complete")
    except Exception as e:
        logger.warning("Initial DevLake blueprint synchronization failed: %s", e)

    consumer_task = asyncio.create_task(kafka_consumer_worker.run())
    sync_task = asyncio.create_task(synchronization_worker.run())

    yield

    logger.info("Shutting down DevLake Trigger Service...")

    consumer_task.cancel()
    sync_task.cancel()
    await asyncio.gather(consumer_task, sync_task, return_exceptions=True)

    await kafka_consumer.stop()
    await kafka_producer.stop()
    logger.info("DevLake Trigger Service shutdown complete")


# -----------------------------------------------------------------------------
# FastAPI App Initialization
# -----------------------------------------------------------------------------

app = FastAPI(
    title="DevLake Trigger Service",
    version="0.1.0",
    lifespan=lifespan,
)

# Attach components to app.state for observability & router access
app.state.webhook_service = webhook_service
app.state.kafka_producer = kafka_producer
app.state.kafka_consumer = kafka_consumer
app.state.blueprint_cache = blueprint_cache
app.state.event_deduplicator = event_deduplicator
app.state.blueprint_lock_manager = blueprint_lock_manager

app.include_router(health_router)
app.include_router(webhook_router)


@app.get("/")
async def root():
    return {
        "message": "DevLake Trigger Service is running!"
    }