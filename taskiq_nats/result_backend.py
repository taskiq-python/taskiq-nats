import pickle
from typing import Any, Coroutine, Dict, Final, Optional, TypeVar, Union

import nats
from taskiq import AsyncResultBackend
from taskiq.abc.result_backend import TaskiqResult

from nats import NATS
from nats.js import JetStreamContext
from nats.js.object_store import ObjectStore
from nats.js.errors import BucketNotFoundError, ObjectNotFoundError
from taskiq.result import TaskiqResult

from taskiq_nats.exceptions import ResultIsMissingError


_ReturnType = TypeVar("_ReturnType")


class NATSObjectStoreResultBackend(AsyncResultBackend[_ReturnType]):
    """Result backend for NATS Object Store."""

    def __init__(
        self,
        servers: str | list[str],
        keep_results: bool = True,
        bucket_name: str = "taskiq_results",
        **connect_options: Any,
    ) -> None:
        """Construct new result backend.
        
        :param servers: NATS servers .
        :param keep_results: flag to not remove results from Redis after reading.
        :param connect_kwargs: additional arguments for nats `connect()` method.
        """
        self.servers: Final = servers
        self.keep_results: Final = keep_results
        self.bucket_name: Final = bucket_name
        self.connect_options: Final = connect_options

        self.nats_client: NATS
        self.nats_jetstream: JetStreamContext
        self.object_store: ObjectStore
    
    async def startup(self) -> None:
        """Create new connection to NATS.
        
        Initialize JetStream context and new ObjectStore instance.
        """
        self.nats_client = await nats.connect(
            servers=self.servers,
            **self.connect_options,
        )
        self.nats_jetstream = self.nats_client.jetstream()

        try:
            self.object_store = await self.nats_jetstream.object_store(self.bucket_name)
        except BucketNotFoundError:
            self.object_store = await self.nats_jetstream.create_object_store(self.bucket_name)
    
    async def shutdown(self) -> None:
        if self.nats_client.is_closed:
            return
        await self.nats_client.close()

    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        await self.object_store.put(
            name=task_id,
            data=pickle.dumps(result),
        )
    
    async def is_result_ready(self, task_id: str) -> bool:
        """Returns whether the result is ready.
        
        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        try:
            await self.object_store.get(name=task_id)
        except ObjectNotFoundError:
            return False
        return True

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Retrieve result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: TaskiqResult.
        """
        try:
            result = await self.object_store.get(
                name=task_id,
            )
        except ObjectNotFoundError as exc:
            raise ResultIsMissingError from exc

        if not self.keep_results:
            await self.object_store.delete(
                name=task_id,
            )
        
        taskiq_result: TaskiqResult[_ReturnType] = pickle.loads(  # noqa: S301
            result.data,
        )

        if not with_logs:
            taskiq_result.log = None
        
        return taskiq_result