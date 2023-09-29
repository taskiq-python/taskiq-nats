import asyncio
import uuid
from typing import List

import pytest
from taskiq import BrokerMessage

from taskiq_nats import JetStreamBroker
from tests.utils import read_message


@pytest.mark.anyio
async def test_success(nats_urls: List[str], nats_subject: str) -> None:
    """
    Tests that JetStream works.

    This function sends a message to JetStream
    before starting to listen to it.

    It epexts to receive the same message.
    """
    broker = JetStreamBroker(
        servers=nats_urls,
        subject=nats_subject,
        queue=uuid.uuid4().hex,
        stream_name=uuid.uuid4().hex,
    )
    await broker.startup()
    sent_message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="meme",
        message=b"some",
        labels={},
    )
    await broker.kick(sent_message)
    assert await asyncio.wait_for(read_message(broker), 0.5) == sent_message.message
    await broker.shutdown()
