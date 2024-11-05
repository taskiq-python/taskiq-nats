import datetime as dt
import uuid
from typing import List

import pytest
from taskiq import ScheduledTask

from taskiq_nats import NATSKeyValueScheduleSource


@pytest.mark.anyio
async def test_set_schedule(nats_urls: List[str]) -> None:
    prefix = uuid.uuid4().hex
    source = NATSKeyValueScheduleSource(servers=nats_urls, prefix=prefix)
    await source.startup()
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.shutdown()


@pytest.mark.anyio
async def test_delete_schedule(nats_urls: List[str]) -> None:
    prefix = uuid.uuid4().hex
    source = NATSKeyValueScheduleSource(servers=nats_urls, prefix=prefix)
    await source.startup()
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.delete_schedule(schedule.schedule_id)
    schedules = await source.get_schedules()
    # Schedules are empty.
    assert not schedules
    await source.shutdown()


@pytest.mark.anyio
async def test_post_run_cron(nats_urls: List[str]) -> None:
    prefix = uuid.uuid4().hex
    source = NATSKeyValueScheduleSource(servers=nats_urls, prefix=prefix)
    await source.startup()
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    assert await source.get_schedules() == [schedule]
    await source.post_send(schedule)
    assert await source.get_schedules() == [schedule]
    await source.shutdown()


@pytest.mark.anyio
async def test_post_run_time(nats_urls: List[str]) -> None:
    prefix = uuid.uuid4().hex
    source = NATSKeyValueScheduleSource(servers=nats_urls, prefix=prefix)
    await source.startup()
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=dt.datetime(2000, 1, 1),
    )
    await source.add_schedule(schedule)
    assert await source.get_schedules() == [schedule]
    await source.post_send(schedule)
    assert await source.get_schedules() == []
    await source.shutdown()
