from logging import getLogger
from typing import Any, AsyncGenerator, Dict, List, Optional, TypeVar, Union

from nats.aio.client import Client
from nats.js import JetStreamContext
from nats.js.api import StreamConfig
from taskiq import AckableMessage, AsyncBroker, BrokerMessage

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
        subject: str = "tasiq_tasks",
        queue: Optional[str] = None,
        **connection_kwargs: Any,
    ) -> None:
        super().__init__()
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


class NatsJetStreamBroker(AsyncBroker):
    """
    NATS broker with JetStream support for taskiq.

    Docs about jetstream:
    https://docs.nats.io/nats-concepts/jetstream
    """

    def __init__(  # noqa: WPS211 (too many args)
        self,
        servers: Union[str, List[str]],
        subject: str = "tasiq_tasks",
        queue: Optional[str] = None,
        jetstream_kwargs: Optional[Dict[str, Any]] = None,
        stream_config: Optional[StreamConfig] = None,
        connection_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__()
        self.servers = servers
        self.client: Client = Client()
        self.connection_kwargs = connection_kwargs or {}
        self.jetstream_kwargs = jetstream_kwargs or {}
        self.queue = queue
        self.subject = subject
        self.jetstream: "JetStreamContext" = None  # type: ignore
        self.stream_config = stream_config or StreamConfig(
            name="taskiq-stream",
            subjects=[self.subject],
        )

    async def startup(self) -> None:
        """
        Startup event handler.

        It simply connects to NATS cluster.
        """
        await super().startup()
        await self.client.connect(self.servers, **self.connection_kwargs)
        self.jetstream = self.client.jetstream()
        await self.jetstream.add_stream(self.stream_config)

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send a message using NATS.

        :param message: message to send.
        """
        await self.jetstream.publish(
            self.subject,
            payload=message.message,
            headers=message.labels,
        )

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Start listen to new messages.

        :yield: incoming messages.
        """
        subscribe = await self.jetstream.subscribe(
            subject=self.subject,
            manual_ack=True,
            queue=self.queue or None,
        )

        async for message in subscribe.messages:
            yield AckableMessage(
                data=message.data,
                ack=message.ack,
                reject=message.nak,
            )

    async def shutdown(self) -> None:
        """Close connections to NATS."""
        await self.client.close()
        await super().shutdown()
