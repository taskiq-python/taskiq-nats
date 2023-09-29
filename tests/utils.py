from taskiq import AckableMessage, AsyncBroker


async def read_message(broker: AsyncBroker) -> bytes | AckableMessage:
    """
    Read signle message from the broker's listen method.

    :param broker: current broker.
    :return: firs message.
    """
    msg: bytes | AckableMessage = b"error"
    async for message in broker.listen():
        msg = message
        break
    return msg
