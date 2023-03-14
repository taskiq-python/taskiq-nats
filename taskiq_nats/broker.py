from typing import Any, AsyncGenerator, Callable, Optional, TypeVar, Union
from taskiq import AsyncBroker, AsyncResultBackend, BrokerMessage

from nats.aio.client import Client

_T = TypeVar("_T")


class NatsBroker(AsyncBroker):
    def __init__(
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
        subscribe = await self.client.subscribe(self.subject, queue=self.queue)
        async for message in subscribe.messages:
            try:
                message = BrokerMessage.parse_raw(message.data)
                yield message
            except ValueError:
                continue

    async def shutdown(self) -> None:
        await self.client.close()
