"""
NATS integration for taskiq.

This package contains broker that
uses NATS as a message queue.
"""

from taskiq_nats.broker import NatsBroker

__all__ = ["NatsBroker"]
