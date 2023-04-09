import asyncio
import uuid
from typing import List

import pytest
from taskiq import BrokerMessage

from taskiq_nats import NatsBroker


async def read_message(broker: NatsBroker) -> bytes:  # type: ignore
    """
    Read signle message from the broker's listen method.

    :param broker: current broker.
    :return: firs message.
    """
    async for message in broker.listen():  # noqa: WPS328
        return message


@pytest.mark.anyio
async def test_success_broadcast(nats_urls: List[str], nats_subject: str) -> None:
    """Test that broadcasting works."""
    broker = NatsBroker(servers=nats_urls, subject=nats_subject)
    await broker.startup()
    tasks = []
    for _ in range(10):
        tasks.append(asyncio.wait_for(asyncio.create_task(read_message(broker)), 1))

    sent_message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="meme",
        message=b"some",
        labels={},
    )
    asyncio.create_task(broker.kick(sent_message))
    for received_message in await asyncio.gather(*tasks):
        assert received_message == sent_message.message


@pytest.mark.anyio
async def test_success_queued(nats_urls: List[str], nats_subject: str) -> None:
    """Testing that queue works."""
    broker = NatsBroker(servers=nats_urls, subject=nats_subject, queue=uuid.uuid4().hex)
    await broker.startup()
    reading_task = asyncio.create_task(
        asyncio.wait_for(read_message(broker), timeout=1),
    )

    sent_message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="meme",
        message=b"some",
        labels={},
    )
    asyncio.create_task(broker.kick(sent_message))
    assert await reading_task == sent_message.message
