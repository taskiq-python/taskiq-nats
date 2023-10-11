from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Final, Generic, List, Optional, Self, TypeVar, Union

from nats.aio.client import Client
from nats.aio.msg import Msg as NatsMessage
from nats.js import JetStreamContext
from nats.js.api import StreamConfig, ConsumerConfig
from nats.errors import TimeoutError as NatsTimeoutError
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

_T = TypeVar("_T")  # noqa: WPS111 (Too short)


JetStreamConsumerType = TypeVar(
    "JetStreamConsumerType",
)


logger = getLogger("taskiq_nats")


class NatsBroker(AsyncBroker):
    """
    NATS broker for taskiq.

    By default this broker works
    broadcasting message to all connected workers.

    If you want to make it work as queue,
    you need to supply name of the queue in
    queue argument.

    Docs about queue:
    https://docs.nats.io/nats-concepts/core-nats/queue
    """

    def __init__(  # noqa: WPS211 (too many args)
        self,
        servers: Union[str, List[str]],
        subject: str = "taskiq_tasks",
        queue: Optional[str] = None,
        result_backend: "Optional[AsyncResultBackend[_T]]" = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(result_backend, task_id_generator)
        self.servers = servers
        self.client: Client = Client()
        self.connection_kwargs = connection_kwargs
        self.queue = queue
        self.subject = subject

    async def startup(self) -> None:
        """
        Startup event handler.

        It simply connects to NATS cluster.
        """
        await super().startup()
        await self.client.connect(self.servers, **self.connection_kwargs)

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send a message using NATS.

        :param message: message to send.
        """
        await self.client.publish(
            self.subject,
            payload=message.message,
            headers=message.labels,
        )

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Start listen to new messages.

        :yield: incoming messages.
        """
        subscribe = await self.client.subscribe(self.subject, queue=self.queue or "")
        async for message in subscribe.messages:
            yield message.data

    async def shutdown(self) -> None:
        """Close connections to NATS."""
        await self.client.close()
        await super().shutdown()


class BaseJetStreamBroker(AsyncBroker, ABC, Generic[JetStreamConsumerType]):
    """Base JetStream broker for taskiq.
    
    It has two subclasses - PullBasedJetStreamBroker
    and PushBasedJetStreamBroker.

    These brokers create a JetStream context
    and use it to send and receive messages.

    This is useful for systems where you need to
    be sure that messages are delivered to the workers.
    """

    def __init__(
        self: Self,
        servers: Union[str, List[str]],
        subject: str = "taskiq_tasks",
        stream_name: str = "taskiq_jetstream",
        queue: Optional[str] = None,
        durable: str = "taskiq_durable",
        result_backend: "Optional[AsyncResultBackend[_T]]" = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        stream_config: Optional[StreamConfig] = None,
        consumer_config: Optional[ConsumerConfig] = None,
        pull_consume_batch: int = 1,
        pull_consume_timeout: Optional[float] = None,
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(result_backend, task_id_generator)
        self.servers = servers
        self.client: Client = Client()
        self.connection_kwargs = connection_kwargs
        self.subject = subject
        self.stream_name = stream_name
        self.js: JetStreamContext
        self.stream_config = stream_config or StreamConfig()
        self.consumer_config = consumer_config

        # Only for push based consumer
        self.queue = queue
        self.default_consumer_name: Final = "taskiq_consumer"
        # Only for pull based consumer
        self.durable = durable
        self.pull_consume_batch = pull_consume_batch
        self.pull_consume_timeout = pull_consume_timeout

        self.consumer: JetStreamConsumerType
    
    async def startup(self) -> None:
        """
        Startup event handler.

        It simply connects to NATS cluster, and
        setup JetStream.
        """
        await super().startup()
        await self.client.connect(self.servers, **self.connection_kwargs)
        self.js = self.client.jetstream()
        if self.stream_config.name is None:
            self.stream_config.name = self.stream_name
        if not self.stream_config.subjects:
            self.stream_config.subjects = [self.subject]
        await self.js.add_stream(config=self.stream_config)
        await self._startup_consumer()
    
    async def shutdown(self) -> None:
        """Close connections to NATS."""
        await self.client.close()
        await super().shutdown()
    
    async def kick(self, message: BrokerMessage) -> None:
        """
        Send a message using NATS.

        :param message: message to send.
        """
        await self.js.publish(
            self.subject,
            payload=message.message,
            headers=message.labels,
        )
    
    @abstractmethod
    async def _startup_consumer(self: Self) -> None:
        """Create consumer."""


class PushBasedJetStreamBroker(
    BaseJetStreamBroker[JetStreamContext.PushSubscription],
):
    """JetStream broker for push based message consumption.
    
    It's named `push` based because nats server push messages to
    the consumer, not consumer requests them.
    """

    async def _startup_consumer(self: Self) -> None:
        if not self.consumer_config:
            self.consumer_config = ConsumerConfig(
                name=self.default_consumer_name,
                durable_name=self.default_consumer_name,
            )

        self.consumer = await self.js.subscribe(
            subject=self.subject,
            queue=self.queue or "",
            config=self.consumer_config,
        )
    
    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Start listen to new messages.

        :yield: incoming messages.
        """
        async for message in self.consumer.messages:
            yield AckableMessage(
                data=message.data,
                ack=message.ack,
            )


class PullBasedJetStreamBroker(
    BaseJetStreamBroker[JetStreamContext.PullSubscription],
):
    """JetStream broker for pull based message consumption.
    
    It's named `pull` based because consumer requests messages,
    not NATS server sends them.
    """

    async def _startup_consumer(self: Self) -> None:
        if not self.consumer_config:
            self.consumer_config = ConsumerConfig(
                durable_name=self.durable,
            )
        
        # We must use this method to create pull based consumer
        # because consumer config won't change without it.
        await self.js.add_consumer(
            stream=self.stream_config.name or self.stream_name,
            config=self.consumer_config,
        )
        self.consumer = await self.js.pull_subscribe(
            subject=self.subject,
            durable=self.durable,
            config=self.consumer_config,
        )
    
    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Start listen to new messages.

        :yield: incoming messages.
        """
        while True:
            try:
                nats_messages: List[NatsMessage] = await self.consumer.fetch(
                    batch=self.pull_consume_batch,
                    timeout=self.pull_consume_timeout,
                )
                for nats_message in nats_messages:
                    yield AckableMessage(
                        data=nats_message.data,
                        ack=nats_message.ack,
                    )
            except NatsTimeoutError:
                continue
