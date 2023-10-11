import asyncio
import uuid
from typing import List

import pytest
from taskiq import AckableMessage, BrokerMessage

from taskiq_nats import PushBasedJetStreamBroker, PullBasedJetStreamBroker
from tests.utils import read_message


@pytest.mark.anyio
async def test_push_based_broker_success(
    nats_urls: List[str],
    nats_subject: str,
) -> None:
    """
    Tests that PushBasedJetStreamBroker works.

    This function sends a message to JetStream
    before starting to listen to it.

    It expects to receive the same message.
    """
    broker = PushBasedJetStreamBroker(
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
    ackable_msg = await asyncio.wait_for(read_message(broker), 0.5)
    assert isinstance(ackable_msg, AckableMessage)
    assert ackable_msg.data == sent_message.message
    ack = ackable_msg.ack()
    if ack is not None:
        await ack
    await broker.js.delete_consumer(
        stream=broker.stream_name,
        consumer=broker.default_consumer_name,
    )
    await broker.js.delete_stream(
        broker.stream_name,
    )
    await broker.shutdown()


@pytest.mark.anyio()
async def test_pull_based_broker_success(
    nats_urls: List[str],
    nats_subject: str,
) -> None:
    """
    Tests that PullBasedJetStreamBroker works.

    This function sends a message to JetStream
    before starting to listen to it.

    It expects to receive the same message.
    """
    broker = PullBasedJetStreamBroker(
        servers=nats_urls,
        subject=nats_subject,
    )
    await broker.startup()
    sent_message = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="meme",
        message=b"some",
        labels={},
    )
    await broker.kick(sent_message)
    ackable_msg = await asyncio.wait_for(read_message(broker), 0.5)
    assert isinstance(ackable_msg, AckableMessage)
    assert ackable_msg.data == sent_message.message
    ack = ackable_msg.ack()
    if ack is not None:
        await ack
    await broker.js.delete_consumer(
        stream=broker.stream_name,
        consumer=broker.default_consumer_name,
    )
    await broker.js.delete_stream(
        broker.stream_name,
    )
    await broker.shutdown()