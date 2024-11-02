import os
import uuid
from typing import AsyncGenerator, Final, List, TypeVar

import pytest
from nats import NATS
from nats.js import JetStreamContext

from taskiq_nats import NATSObjectStoreResultBackend

_ReturnType = TypeVar("_ReturnType")


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
    nats_urls: List[str],
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


@pytest.fixture()
async def nats_result_backend(
    nats_urls: List[str],
) -> AsyncGenerator[NATSObjectStoreResultBackend[_ReturnType], None]:
    backend: NATSObjectStoreResultBackend[_ReturnType] = NATSObjectStoreResultBackend(
        servers=nats_urls,
    )
    await backend.startup()
    yield backend
    await backend.shutdown()
