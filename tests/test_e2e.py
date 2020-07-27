import sys
from datetime import datetime
from time import sleep
from typing import Callable

from queueless import client, log, worker
from queueless.task import TaskStatus
from tests.services import start_local_postgres_docker_db


def run_test_e2e(db_url):
    client.startup(db_url)
    worker.start_local_workers(n_workers=1, db_url=db_url, worker_tag="tag A")
    worker.start_local_workers(
        n_workers=4, db_url=db_url, worker_tag="tag B", cleanup_timeout=1
    )
    func = _make_test_function()

    # run a simple task
    task_id = client.submit(func, {"param": "abc"}, 123, requires_tag="tag B")
    _wait_for_true(lambda: client.get_task_result(task_id) is not None)
    result = client.get_task_result(task_id)
    assert result == len("abc") + 42
    log.log("[OK] Tasks run")

    # Tasks are rescheduled if their worker is dead
    # start a task which takes longer than the expected heartbeat,
    # resulting in the worker being considered 'dead' and its task
    # rescheduled, this should happen `n_retries` times
    task_id = client.submit(_sleep, {"seconds": 5}, 123, n_retries_if_worker_hangs=2)
    _wait_for_true(lambda: client.get_task_status(task_id) == TaskStatus.TIMEOUT)
    log.log("[OK] Orphaned tasks rescheduled")

    log.log("[OK] All OK! :)")


def _wait_for_true(func, timeout_seconds=10):
    start = datetime.now()
    while not func():
        sleep(0.1)
        if (datetime.now() - start).total_seconds() > timeout_seconds:
            raise TimeoutError(f"Waited for longer than {timeout_seconds} seconds.")


def _sleep(seconds: float) -> None:
    from time import sleep

    sleep(seconds)


def _make_test_function() -> Callable[[str], int]:
    """ Create a test function with a couple of complex elements:
    - a class definition which will be referenced in the function
        closure (ClosuredClass)
    - an instance of this class, whose state determines the funciton
        behaviour (the field x=42)
    """

    class ClosuredClass:
        def __init__(self, x: int) -> None:
            self.x = x

    closured_obj = ClosuredClass(x=42)

    def closured(param: str) -> int:
        return len(param) + closured_obj.x

    return closured


if __name__ == "__main__":
    if len(sys.argv) < 2:
        db_url = start_local_postgres_docker_db()
    else:
        db_url = sys.argv[1]

    run_test_e2e(db_url)
