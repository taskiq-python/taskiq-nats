import subprocess
import pytest
import time

from tests.path.broker import my_task


@pytest.mark.anyio
async def test_path():
    process_worker = subprocess.Popen(
        ["taskiq", "worker", "tests.path.broker:broker", "--workers", "1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )

    time.sleep(3)

    process_broker = subprocess.Popen(
        ["python", "tests/path/broker.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )

    time.sleep(3)

    return_code = process_broker.wait()
    out, err = process_broker.communicate()

    assert return_code == 0, f"Process broker failed with error: {err.decode()}"
    assert not err.decode(), f"Process broker had errors: {err.decode()}"
    assert not out.decode(), f"Process broker had errors: {out.decode()}"

    #
    # out, err = process_worker.communicate()
    # print('OUT')
    # print(out.decode())
    # print('ERR')
    # print(err.decode())
    # assert err == ''
