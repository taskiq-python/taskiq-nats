import asyncio

from taskiq_nats import NatsBroker, NATSObjectStoreResultBackend


result_backend = NATSObjectStoreResultBackend(servers=["nats://localhost:4222"])
broker = NatsBroker(servers=["nats://localhost:4222"]).with_result_backend(result_backend)

@broker.task
async def my_task(arg1: int, arg2: str) -> int:
    print("Hello from my_task!", arg1, arg2)
    return arg1


# usage
async def main():
    await broker.startup()
    task = await my_task.kiq(arg1=1, arg2="world")
    task_result = await task.wait_result()

    if task_result.return_value != 1:
        raise Exception(f"task_result.return_value != 1, return value = {task_result.return_value}")

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
