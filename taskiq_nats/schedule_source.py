import logging
from typing import Any, Final, List, Optional, Union

import nats
from nats import NATS
from nats.js import JetStreamContext
from nats.js.errors import BucketNotFoundError, NoKeysError
from nats.js.kv import KeyValue
from taskiq import ScheduledTask, ScheduleSource
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.serializers import PickleSerializer

log = logging.getLogger(__name__)


class NATSKeyValueScheduleSource(ScheduleSource):
    """
    Source of schedules for NATS Key-Value storage.

    This class allows you to store schedules in NATS Key-Value storage.
    Also it supports dynamic schedules.
    """

    def __init__(
        self,
        servers: Union[str, List[str]],
        bucket_name: str = "taskiq_schedules",
        prefix: str = "schedule",
        serializer: Optional[TaskiqSerializer] = None,
        **connect_options: Any,
    ) -> None:
        """Construct new result backend.

        :param servers: NATS servers.
        :param bucket_name: name of the bucket where schedules would be stored.
        :param prefix: prefix for nats kv storage schedule keys.
        :param serializer: serializer for data.
        :param connect_kwargs: additional arguments for nats `connect()` method.
        """
        self.servers: Final = servers
        self.bucket_name: Final = bucket_name
        self.prefix: Final = prefix
        self.serializer = serializer or PickleSerializer()
        self.connect_options: Final = connect_options

        self.nats_client: NATS
        self.nats_jetstream: JetStreamContext
        self.kv: KeyValue

    async def startup(self) -> None:
        """Create new connection to NATS.

        Initialize JetStream context and new KeyValue instance.
        """
        self.nats_client = await nats.connect(
            servers=self.servers,
            **self.connect_options,
        )
        self.nats_jetstream = self.nats_client.jetstream()

        try:
            self.kv = await self.nats_jetstream.key_value(self.bucket_name)
        except BucketNotFoundError:
            self.kv = await self.nats_jetstream.create_key_value(
                bucket=self.bucket_name,
            )

    async def shutdown(self) -> None:
        """Close nats connection."""
        if self.nats_client.is_closed:
            return
        await self.nats_client.close()

    async def delete_schedule(self, schedule_id: str) -> None:
        """Remove schedule by id."""
        await self.kv.delete(f"{self.prefix}.{schedule_id}")

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """
        Add schedule to NATS Key-Value storage.

        :param schedule: schedule to add.
        :param schedule_id: schedule id.
        """
        await self.kv.put(
            f"{self.prefix}.{schedule.schedule_id}",
            self.serializer.dumpb(model_dump(schedule)),
        )

    async def get_schedules(self) -> List[ScheduledTask]:
        """
        Get all schedules from NATS Key-Value storage.

        This method is used by scheduler to get all schedules.

        :return: list of schedules.
        """
        try:
            schedules = await self.kv.history(f"{self.prefix}.*")
        except NoKeysError:
            return []

        return [
            model_validate(ScheduledTask, self.serializer.loadb(schedule.value))
            for schedule in schedules
            if schedule and schedule.value
        ]

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)
