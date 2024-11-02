"""
NATS integration for taskiq.

This package contains brokers that
uses NATS as a message queue.
"""

from taskiq_nats.broker import (
    NatsBroker,
    PullBasedJetStreamBroker,
    PushBasedJetStreamBroker,
)
from taskiq_nats.result_backend import NATSObjectStoreResultBackend

__all__ = [
    "NatsBroker",
    "PushBasedJetStreamBroker",
    "PullBasedJetStreamBroker",
    "NATSObjectStoreResultBackend",
]
