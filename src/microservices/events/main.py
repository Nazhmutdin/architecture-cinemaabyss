import os
import json
import uvicorn
import logging
import asyncio
from typing import List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from typing import Callable, Coroutine, Any
from datetime import datetime, timezone, date
from pydantic import BaseModel, Field

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI(
    title="CinemaAbyss - Events Service",
    version="0.1.0",
    docs_url="/docs",
    openapi_url="/openapi.json",
)

kafka_logger = logging.getLogger("events.kafka")
app_logger = logging.getLogger("events.service")


def utc_now():
    return datetime.now(timezone.utc)

class EventResponse(BaseModel):
    status: str
    partition: int
    offset: int
    event: dict


class UserEvent(BaseModel):
    user_id: int
    username: str
    email: str | None = Field(default=None)
    action: str
    timestamp: datetime = Field(default_factory=utc_now)


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    currency: str | None = Field(default="RUB")
    status: str | None = Field(default="completed")
    method_type: str | None = Field(default=None)
    timestamp: datetime = Field(default_factory=utc_now)


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int
    rating: float | None = Field(default=None)
    genres: List[str] | None = Field(default=None)
    timestamp: datetime = Field(default_factory=utc_now)



KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS = os.getenv("TOPICS", "user-events,payment-events,movie-events").split(",")


def _json_default_convertor(obj):
    if isinstance(obj, datetime):
        return obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(obj, date):
        return obj.isoformat()
    return str(obj)


class KafkaClient:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

        self.producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP)
        self.consumer = AIOKafkaConsumer(
            *TOPICS,
            loop=loop,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="events-service-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )

        self._consumer_task = None


    async def start(self, message_handler: Callable[[str, dict], Coroutine[Any,Any, None]]):
        kafka_logger.info("Starting Kafka producer and consumer. \nBrokers=%s \nTopics=%s\n\n", KAFKA_BOOTSTRAP, TOPICS)

        await self.producer.start()
        await self.consumer.start()

        self._consumer_task = asyncio.create_task(self._consume_loop(message_handler))

        kafka_logger.info("Kafka started")


    async def stop(self):
        kafka_logger.info("Stopping Kafka client")

        if self._consumer_task:
            self._consumer_task.cancel()

            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        await self.consumer.stop()
        await self.producer.stop()

        kafka_logger.info("Kafka stopped")


    async def send(self, topic: str, value: dict, key: str | None = None):
        payload = json.dumps(value, default=_json_default_convertor).encode("utf-8")

        fut = await self.producer.send_and_wait(topic, payload, key=(key.encode("utf-8") if key else None))
        partition = fut.partition
        offset = fut.offset

        kafka_logger.info("Produced to %s partition=%s offset=%s key=%s", topic, partition, offset, key)

        return partition, offset


    async def _consume_loop(self, handler):
        try:
            async for msg in self.consumer:

                try:
                    val = msg.value.decode("utf-8")
                    data = json.loads(val)
                except Exception as e:
                    kafka_logger.exception("Failed to decode message: %s", e)
                    continue

                topic = msg.topic
                kafka_logger.debug("Consumed message topic=%s partition=%s offset=%s", msg.topic, msg.partition, msg.offset)

                try:
                    await handler(topic, data)
                except Exception:
                    kafka_logger.exception("Error in message handler")

        except asyncio.CancelledError:
            kafka_logger.info("Consumer loop cancelled, exiting")

        except Exception:
            kafka_logger.exception("Consumer loop error")

loop = asyncio.get_event_loop()
kafka_client = KafkaClient(loop=loop)


async def internal_message_handler(topic: str, data: dict):
    app_logger.info("INTERNAL HANDLER | topic=%s | event=%s\n", topic, data)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await kafka_client.start(internal_message_handler)
        app_logger.info("Kafka client started in lifespan startup")
    except Exception:
        app_logger.exception("Failed to start Kafka client at startup")

    try:
        yield
    finally:
        try:
            await kafka_client.stop()
            app_logger.info("Kafka client stopped in lifespan shutdown")
        except Exception:
            app_logger.exception("Error while stopping Kafka client")


app.router.lifespan_context = lifespan


class IDGenerator:
    def __init__(self):
        self._counters = {
            "user": 0,
            "payment": 0,
            "movie": 0,
        }
        self._locks = {k: asyncio.Lock() for k in self._counters}

    async def next(self, group: str) -> int:
        if group not in self._counters:
            raise ValueError(f"Unknown id group: {group}")
        async with self._locks[group]:
            self._counters[group] += 1
            return self._counters[group]


id_gen = IDGenerator()


@app.get("/api/events/health")
async def health():
    status = True
    return {"status": status, "kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")}

@app.post("/api/events/user", response_model=EventResponse, status_code=201)
async def create_user_event(event: UserEvent):
    topic = "user-events"

    try:
        new_id = await id_gen.next("user")
        payload = {
            "id": new_id,
            "type": "user",
            "timestamp": event.timestamp.isoformat(),
            "payload": event.model_dump(),
        }

        partition, offset = await kafka_client.send(topic, payload, key=str(event.user_id))

        return {"status": "success", "partition": partition, "offset": offset, "event": payload}
    
    except Exception as e:
        app_logger.exception("Failed to produce user-event")

        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/payment", response_model=EventResponse, status_code=201)
async def create_payment_event(event: PaymentEvent):
    topic = "payment-events"

    try:
        payload = {
            "id": event.payment_id,
            "type": "payment",
            "timestamp": event.timestamp.isoformat(),
            "payload": event.model_dump(),
        }

        partition, offset = await kafka_client.send(topic, payload, key=str(event.payment_id))

        return {"status": "success", "partition": partition, "offset": offset, "event": payload}
    
    except Exception as e:
        app_logger.exception("Failed to produce payment-event")

        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/movie", response_model=EventResponse, status_code=201)
async def create_movie_event(event: MovieEvent):
    topic = "movie-events"

    try:
        new_id = await id_gen.next("movie")
        payload = {
            "id": new_id,
            "type": "movie",
            "timestamp": event.timestamp.isoformat(),
            "payload": event.model_dump(),
        }
        partition, offset = await kafka_client.send(topic, payload, key=str(event.movie_id))

        return {"status": "success", "partition": partition, "offset": offset, "event": payload}
    except Exception as e:

        app_logger.exception("Failed to produce movie-event")

        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8082")), reload=False)