from taskiq import AsyncBroker


async def read_message(broker: AsyncBroker) -> bytes:  # type: ignore
    """
    Read signle message from the broker's listen method.

    :param broker: current broker.
    :return: firs message.
    """
    async for message in broker.listen():
        assert isinstance(message, bytes)
        return message
