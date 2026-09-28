import asyncio
import uuid
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

from taskiq import AckableMessage, BrokerMessage

from taskiq_nats import PullBasedJetStreamBroker, PushBasedJetStreamBroker
from tests.utils import read_message


async def test_push_based_broker_exposes_ack_progress(
    nats_urls: list[str],
) -> None:
    nats_message = MagicMock()
    nats_message.data = b"message"
    nats_message.ack = AsyncMock()
    nats_message.in_progress = AsyncMock()

    async def message_stream() -> AsyncGenerator[MagicMock, None]:
        yield nats_message

    broker = PushBasedJetStreamBroker(servers=nats_urls)
    broker.consumer = MagicMock(messages=message_stream())

    message = await anext(broker.listen())

    assert message.ack_progress is not None
    await message.ack_progress()
    nats_message.in_progress.assert_awaited_once_with()


async def test_pull_based_broker_exposes_ack_progress(
    nats_urls: list[str],
) -> None:
    nats_message = MagicMock()
    nats_message.data = b"message"
    nats_message.ack = AsyncMock()
    nats_message.in_progress = AsyncMock()

    broker = PullBasedJetStreamBroker(servers=nats_urls)
    broker.consumer = MagicMock()
    broker.consumer.fetch = AsyncMock(return_value=[nats_message])

    message = await anext(broker.listen())

    assert message.ack_progress is not None
    await message.ack_progress()
    nats_message.in_progress.assert_awaited_once_with()


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
    assert ackable_msg.ack_progress is not None
    await ackable_msg.ack_progress()
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
    assert ackable_msg.ack_progress is not None
    await ackable_msg.ack_progress()
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
