# Taskiq NATS

Taskiq-nats is a plugin for taskiq that adds NATS broker.

## Installation

To use this project you must have installed core taskiq library:

```bash
pip install taskiq taskiq-redis
```

## Usage

Here's a minimal setup example with a broker and one task.

```python
import asyncio
from taskiq_nats import NatsBroker

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

## NatsBroker configuration

Here's the constructor parameters:

* `servers` - a single string or a list of strings with nats nodes addresses.
* `subject` - name of the subect that will be used to exchange tasks betwee workers and clients.
* `queue` - optional name of the queue. By default NatsBroker broadcasts task to all workers,
    but if you want to handle every task only once, you need to supply this argument.
* `result_backend` - custom result backend.
* `task_id_generator` - custom function to generate task ids.
* Every other keyword argument will be sent to `nats.connect` function.
