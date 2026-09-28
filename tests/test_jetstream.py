import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from nats.js import JetStreamContext
from nats.js.api import RetentionPolicy, StreamConfig
from nats.js.errors import BadRequestError
from taskiq import AckableMessage, BrokerMessage

from taskiq_nats import PullBasedJetStreamBroker, PushBasedJetStreamBroker
from tests.utils import read_message


async def test_push_based_broker_success(  # (too many await)
    nats_urls: list[str],
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


async def test_pull_based_broker_success(
    nats_urls: list[str],
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
        consumer=broker.durable,
    )
    await broker.js.delete_stream(
        broker.stream_name,
    )
    await broker.shutdown()


async def test_broker_reuses_existing_stream(
    nats_urls: list[str],
    nats_subject: str,
    nats_jetstream: JetStreamContext,
) -> None:
    stream_name = uuid.uuid4().hex
    await nats_jetstream.add_stream(
        config=StreamConfig(
            name=stream_name,
            subjects=[nats_subject],
            retention=RetentionPolicy.WORK_QUEUE,
            max_msgs=1000,
        ),
    )

    broker = PullBasedJetStreamBroker(
        servers=nats_urls,
        subject=nats_subject,
        stream_name=stream_name,
    )
    await broker.startup()

    stream_info = await broker.js.stream_info(stream_name)
    assert stream_info.config.retention == RetentionPolicy.WORK_QUEUE
    assert stream_info.config.max_msgs == 1000

    await broker.js.delete_consumer(
        stream=stream_name,
        consumer=broker.durable,
    )
    await broker.js.delete_stream(stream_name)
    await broker.shutdown()


async def test_broker_startup_reraises_unknown_stream_error(
    nats_urls: list[str],
    nats_subject: str,
) -> None:
    broker = PullBasedJetStreamBroker(
        servers=nats_urls,
        subject=nats_subject,
        stream_name=uuid.uuid4().hex,
    )
    broker.js = MagicMock()
    broker.js.add_stream = AsyncMock(
        side_effect=BadRequestError(code=400, err_code=10052),
    )

    with pytest.raises(BadRequestError):
        await broker._add_or_reuse_stream()
