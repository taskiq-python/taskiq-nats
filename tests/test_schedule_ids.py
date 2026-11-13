import datetime as dt
import pickle
import uuid
from collections.abc import AsyncGenerator

import pytest
from taskiq import ScheduledTask
from taskiq.cli.scheduler.run import get_schedules

from taskiq_nats import NATSKeyValueScheduleSource


@pytest.fixture
async def schedule_source(
    nats_urls: list[str],
) -> AsyncGenerator[NATSKeyValueScheduleSource, None]:
    source = NATSKeyValueScheduleSource(
        servers=nats_urls,
        bucket_name=f"schedules_{uuid.uuid4().hex}",
        prefix="schedules",
    )
    await source.startup()
    try:
        yield source
    finally:
        try:
            await source.nats_jetstream.delete_key_value(source.bucket_name)
        finally:
            await source.shutdown()


@pytest.mark.parametrize("prefix", ["schedules", "team.schedules"])
@pytest.mark.parametrize("kind", ["cron", "time"])
async def test_dotted_schedule_ids_round_trip(
    nats_urls: list[str],
    schedule_source: NATSKeyValueScheduleSource,
    prefix: str,
    kind: str,
) -> None:
    source = NATSKeyValueScheduleSource(
        servers=nats_urls,
        bucket_name=schedule_source.bucket_name,
        prefix=prefix,
    )
    await source.startup()
    try:
        schedules = [
            ScheduledTask(
                schedule_id=name,
                task_name="report",
                labels={},
                args=[],
                kwargs={},
                cron="* * * * *" if kind == "cron" else None,
                time=(
                    dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc)
                    if kind == "time"
                    else None
                ),
            )
            for name in ["plain", "daily.report", "team.daily.report"]
        ]
        for schedule in schedules:
            await source.add_schedule(schedule)

        assert sorted(await source.kv.keys()) == sorted(
            f"{prefix}.{task.schedule_id}" for task in schedules
        )
        assert {task.schedule_id: task for task in await source.get_schedules()} == {
            task.schedule_id: task for task in schedules
        }
        assert {task.schedule_id for task in await get_schedules(source)} == {
            task.schedule_id for task in schedules
        }

        await source.delete_schedule("daily.report")
        assert {task.schedule_id for task in await source.get_schedules()} == {
            "plain",
            "team.daily.report",
        }
        restored = schedules[1].model_copy(update={"args": ["updated"]})
        await source.add_schedule(restored)
        snapshot = {task.schedule_id: task for task in await source.get_schedules()}
        assert snapshot[restored.schedule_id] == restored

        await source.post_send(restored)
        assert (
            restored.schedule_id
            in {task.schedule_id for task in await get_schedules(source)}
        ) == (kind == "cron")
    finally:
        await source.shutdown()


async def test_nested_prefix_keeps_its_own_schedules(
    nats_urls: list[str],
    schedule_source: NATSKeyValueScheduleSource,
) -> None:
    nested = NATSKeyValueScheduleSource(
        servers=nats_urls,
        bucket_name=schedule_source.bucket_name,
        prefix=f"{schedule_source.prefix}.nested",
    )
    await nested.startup()
    try:
        root_task = ScheduledTask(
            schedule_id="root_task",
            task_name="report",
            labels={},
            args=[],
            kwargs={},
            cron="* * * * *",
        )
        nested_task = root_task.model_copy(update={"schedule_id": "nested_task"})
        await schedule_source.add_schedule(root_task)
        await nested.add_schedule(nested_task)
        assert await schedule_source.get_schedules() == [root_task]
        assert await nested.get_schedules() == [nested_task]
    finally:
        await nested.shutdown()


async def test_invalid_payload_still_raises_and_recovers(
    schedule_source: NATSKeyValueScheduleSource,
) -> None:
    key = f"{schedule_source.prefix}.broken"
    await schedule_source.kv.put(key, b"not-pickle")
    with pytest.raises(pickle.UnpicklingError):
        await schedule_source.get_schedules()
    await schedule_source.kv.delete(key)
    assert await schedule_source.get_schedules() == []
