from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Optional, TypeVar, Union

from nats.aio.client import Client
from taskiq import AsyncBroker, AsyncResultBackend, BrokerMessage

_T = TypeVar("_T")  # noqa: WPS111 (Too short)


logger = getLogger("taskiq_nats")


class NatsBroker(AsyncBroker):
    def __init__(  # noqa: WPS211 (too many args)
        self,
        servers: Union[str, list[str]],
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
        await super().startup()
        await self.client.connect(self.servers, **self.connection_kwargs)

    async def kick(self, message: BrokerMessage) -> None:
        await self.client.publish(
            self.subject,
            payload=message.json().encode(),
            headers=message.labels,
        )

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:
        subscribe = await self.client.subscribe(self.subject, queue=self.queue or "")
        async for message in subscribe.messages:
            try:
                yield BrokerMessage.parse_raw(message.data)
            except ValueError:
                logger.warning(f"Cannot parse message: {message.data.decode('utf-8')}")

    async def shutdown(self) -> None:
        await self.client.close()
