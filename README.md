# Taskiq NATS

Taskiq-nats is a plugin for taskiq that adds NATS broker.
This package has support for NATS JetStream.

## Installation

To use this project you must have installed core taskiq library:

```bash
pip install taskiq taskiq-nats
```

## Usage

Here's a minimal setup example with a broker and one task.

### Default NATS broker.
```python
import asyncio
from taskiq_nats import NatsBroker, JetStreamBroker

broker = NatsBroker(
    [
        "nats://nats1:4222",
        "nats://nats2:4222",
    ],
    queue="random_queue_name",
)


@broker.task
async def my_lovely_task():
    print("I love taskiq")


async def main():
    await broker.startup()

    await my_lovely_task.kiq()

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

```
### NATS broker based on JetStream
```python
import asyncio
from taskiq_nats import (
    PushBasedJetStreamBroker,
    PullBasedJetStreamBroker
)

broker = PushBasedJetStreamBroker(
    servers=[
        "nats://nats1:4222",
        "nats://nats2:4222",
    ],
    queue="awesome_queue_name",
)

# Or you can use pull based variant
broker = PullBasedJetStreamBroker(
    servers=[
        "nats://nats1:4222",
        "nats://nats2:4222",
    ],
    durable="awesome_durable_consumer_name",
)


@broker.task
async def my_lovely_task():
    print("I love taskiq")


async def main():
    await broker.startup()

    await my_lovely_task.kiq()

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

## NatsBroker configuration

Here's the constructor parameters:

* `servers` - a single string or a list of strings with nats nodes addresses.
* `subject` - name of the subect that will be used to exchange tasks betwee workers and clients.
* `queue` - optional name of the queue. By default NatsBroker broadcasts task to all workers,
    but if you want to handle every task only once, you need to supply this argument.
* `result_backend` - custom result backend.
* `task_id_generator` - custom function to generate task ids.
* Every other keyword argument will be sent to `nats.connect` function.

## JetStreamBroker configuration
### Common
* `servers` - a single string or a list of strings with nats nodes addresses.
* `subject` - name of the subect that will be used to exchange tasks betwee workers and clients.
* `stream_name` - name of the stream where subjects will be located.
* `queue` - a single string or a list of strings with nats nodes addresses.
* `result_backend` - custom result backend.
* `task_id_generator` - custom function to generate task ids.
* `stream_config` - a config for stream.
* `consumer_config` - a config for consumer.

### PushBasedJetStreamBroker
* `queue` - name of the queue. It's used to share messages between different consumers.

### PullBasedJetStreamBroker
* `durable` - durable name of the consumer. It's used to share messages between different consumers.
* `pull_consume_batch` - maximum number of message that can be fetched each time.
* `pull_consume_timeout` - timeout for messages fetch. If there is no messages, we start fetching messages again.


## NATS Result Backend
It's possible to use NATS JetStream to store tasks result.
```python
import asyncio
from taskiq_nats import PullBasedJetStreamBroker
from taskiq_nats.result_backend import NATSObjectStoreResultBackend


result_backend = NATSObjectStoreResultBackend(
    servers="localhost",
)
broker = PullBasedJetStreamBroker(
    servers="localhost",
).with_result_backend(
    result_backend=result_backend,
)


@broker.task
async def awesome_task() -> str:
    return "Hello, NATS!"


async def main() -> None:
    await broker.startup()
    task = await awesome_task.kiq()
    res = await task.wait_result()
    print(res)
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

```
