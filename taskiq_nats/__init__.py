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
from taskiq_nats.schedule_source import NATSKeyValueScheduleSource

__all__ = [
    "NatsBroker",
    "PushBasedJetStreamBroker",
    "PullBasedJetStreamBroker",
    "NATSObjectStoreResultBackend",
    "NATSKeyValueScheduleSource",
]
