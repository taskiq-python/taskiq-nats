from typing import Union

from taskiq import AckableMessage, AsyncBroker


async def read_message(broker: AsyncBroker) -> Union[bytes, AckableMessage]:
    """
    Read single message from the broker's listen method.

    :param broker: current broker.
    :return: first message.
    """
    msg: Union[bytes, AckableMessage] = b"error"
    async for message in broker.listen():
        msg = message
        break
    return msg
