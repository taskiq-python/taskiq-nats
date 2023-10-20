import os
import uuid
from typing import AsyncGenerator, Final, List

import pytest
from nats import NATS
from nats.js import JetStreamContext


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture
def nats_subject() -> str:
    """
    Name of a subject for current test.

    :return: random string.
    """
    return uuid.uuid4().hex


@pytest.fixture
def nats_urls() -> List[str]:
    """
    List of nats urls.

    It tries to parse list from NATS_URLS env,
    if it's none, uses default one.

    :return: list of nats urls.
    """
    urls = os.environ.get("NATS_URLS") or "nats://localhost:4222"
    return urls.split(",")


@pytest.fixture()
async def nats_jetstream(
    nats_urls: List[str],  # noqa: WPS442
) -> AsyncGenerator[JetStreamContext, None]:
    """Create and yield nats client and jetstream instances.

    :param nats_urls: urls to nats.

    :yields: NATS JetStream.
    """
    nats: Final = NATS()
    await nats.connect(servers=nats_urls)
    jetstream: Final = nats.jetstream()
    yield jetstream
    await nats.close()
