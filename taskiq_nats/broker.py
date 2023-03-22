from logging import getLogger
from typing import Any, AsyncGenerator, Callable, List, Optional, TypeVar, Union

from nats.aio.client import Client
from taskiq import AsyncBroker, AsyncResultBackend, BrokerMessage

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
            payload=message.json().encode(),
            headers=message.labels,
        )

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:
        """
        Start listen to new messages.

        :yield: incoming messages.
        """
        subscribe = await self.client.subscribe(self.subject, queue=self.queue or "")
        async for message in subscribe.messages:
            try:
                yield BrokerMessage.parse_raw(message.data)
            except ValueError:
                data = message.data.decode("utf-8")
                logger.warning(f"Cannot parse message: {data}")

    async def shutdown(self) -> None:
        """Close connections to NATS."""
        await self.client.close()
        await super().shutdown()
