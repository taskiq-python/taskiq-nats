from logging import getLogger
from typing import Any, AsyncGenerator, Callable, List, Optional, TypeVar, Union

from nats.aio.client import Client
from nats.js import JetStreamContext
from nats.js.api import StreamConfig
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

_T = TypeVar("_T")  # noqa: WPS111 (Too short)


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


class JetStreamBroker(AsyncBroker):  # noqa: WPS230
    """
    JetStream broker for taskiq.

    This broker creates a JetStream context
    and uses it to send and receive messages.

    This is useful for systems where you need to
    be sure that messages are delivered to the workers.
    """

    def __init__(  # noqa: WPS211 (too many args)
        self,
        servers: Union[str, List[str]],
        subject: str = "tasiq_tasks",
        stream_name: str = "taskiq_jstream",
        queue: Optional[str] = None,
        result_backend: "Optional[AsyncResultBackend[_T]]" = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        stream_config: Optional[StreamConfig] = None,
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(result_backend, task_id_generator)
        self.servers = servers
        self.client: Client = Client()
        self.connection_kwargs = connection_kwargs
        self.queue = queue
        self.subject = subject
        self.stream_name = stream_name
        self.js: JetStreamContext
        self.stream_config = stream_config or StreamConfig()

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

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Start listen to new messages.

        :yield: incoming messages.
        """
        subscribe = await self.js.subscribe(self.subject, queue=self.queue or "")
        async for message in subscribe.messages:
            yield AckableMessage(
                data=message.data,
                ack=message.ack,
            )

    async def shutdown(self) -> None:
        """Close connections to NATS."""
        await self.client.close()
        await super().shutdown()
