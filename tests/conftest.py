import os
from typing import List

import pytest


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
    return "12345"


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
