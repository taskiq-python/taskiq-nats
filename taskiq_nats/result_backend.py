from typing import Any, Final, TypeVar

import nats
from nats.aio.client import Client
from nats.js import JetStreamContext
from nats.js.errors import BucketNotFoundError, ObjectNotFoundError
from nats.js.object_store import ObjectStore
from taskiq import AsyncResultBackend, ResultGetError
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.result import TaskiqResult
from taskiq.serializers import PickleSerializer

_ReturnType = TypeVar("_ReturnType")


class NATSObjectStoreResultBackend(AsyncResultBackend[_ReturnType]):
    """Result backend for NATS Object Store."""

    def __init__(
        self,
        servers: str | list[str],
        keep_results: bool = True,
        bucket_name: str = "taskiq_results",
        serializer: TaskiqSerializer | None = None,
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
        self.serializer = serializer or PickleSerializer()
        self.connect_options: Final = connect_options

        self.nats_client: Client
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
            self.object_store = await self.nats_jetstream.create_object_store(
                self.bucket_name,
            )

    async def shutdown(self) -> None:
        """Close nats connection."""
        if self.nats_client.is_closed:
            return
        await self.nats_client.close()

    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        """Set result to the nats bucket.

        :param task_id: ID of the task.
        :param result: result of the task.
        """
        await self.object_store.put(
            name=task_id,
            data=self.serializer.dumpb(result.model_dump(mode="json")),
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
            raise ResultGetError from exc

        if not self.keep_results:
            await self.object_store.delete(
                name=task_id,
            )

        taskiq_result: TaskiqResult[_ReturnType] = TaskiqResult[
            _ReturnType
        ].model_validate(
            self.serializer.loadb(result.data),  # type: ignore[arg-type]
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[Any],
    ) -> None:
        """Set progress of the task to the nats bucket.

        :param task_id: ID of the task.
        :param progress: progress of the task.
        """
        await self.object_store.put(
            name=f"progress:{task_id}",
            data=self.serializer.dumpb(progress.model_dump(mode="json")),
        )

    async def get_progress(self, task_id: str) -> TaskProgress[Any] | None:
        """Retrieve progress of the task from the nats bucket.

        :param task_id: ID of the task.

        :return: progress of the task or None if it is not set.
        """
        try:
            result = await self.object_store.get(name=f"progress:{task_id}")
        except ObjectNotFoundError:
            return None
        return TaskProgress[Any].model_validate(
            self.serializer.loadb(result.data),  # type: ignore[arg-type]
        )
