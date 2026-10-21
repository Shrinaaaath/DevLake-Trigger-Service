from fastapi import APIRouter, Request, Response, status

router = APIRouter()


@router.get("/health")
async def health():
    """Liveness probe to check if the application process is running."""
    return {"status": "ok"}


@router.get("/readiness")
async def readiness(request: Request, response: Response):
    """Readiness probe to check if dependencies and background tasks are ready."""
    kafka_producer = getattr(request.app.state, "kafka_producer", None)
    kafka_consumer = getattr(request.app.state, "kafka_consumer", None)
    blueprint_cache = getattr(request.app.state, "blueprint_cache", None)

    producer_ok = kafka_producer is not None and kafka_producer.is_connected
    consumer_ok = kafka_consumer is not None and kafka_consumer.is_connected
    cache_ok = blueprint_cache is not None and blueprint_cache.is_initialized

    is_ready = producer_ok and consumer_ok and cache_ok

    result = {
        "status": "ready" if is_ready else "not_ready",
        "components": {
            "kafka_producer": "connected" if producer_ok else "disconnected",
            "kafka_consumer": "connected" if consumer_ok else "disconnected",
            "blueprint_cache": "initialized" if cache_ok else "uninitialized",
        },
    }

    if not is_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return result
