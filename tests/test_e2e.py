import os
from datetime import datetime
from multiprocessing import Process
from time import sleep
from typing import Callable, List

from qless import client, sql, log

# A Script to test the library functionality end to end
from qless.task import TaskStatus


def run_test_e2e():
    db_url = _start_local_postgres_docker_db()
    sql.startup(db_url)
    # sql.reset()
    w = _start_workers(1)
    func = _make_test_function()
    task_id = client.submit(func, {"param": "abc"}, 123)
    sleep(3)
    result = client.get_task_result(task_id)
    assert result == len("abc") + 42
    log.log("Tasks run OK")

    # Tasks are resheduled if a worker dies (TODO)
    # task_id = client.submit(_sleep, {"seconds": 5}, 123)
    # _wait_for_true(lambda : client.get_task_status(task_id) == TaskStatus.RUNNING)
    # w[0].stop()
    # _wait_for_true(lambda : client.get_task_status(task_id) == TaskStatus.PENDING)
    # assert client.get_task_retries(task_id) > 0


def _wait_for_true(func, timeout_seconds=10):
    start = datetime.now()
    while not func():
        sleep(0.1)
        if (datetime.now() - start).total_seconds() > timeout_seconds:
            return


def _sleep(seconds: float) -> None:
    sleep(seconds)


def _make_test_function() -> Callable[[str], int]:
    """ Create a test function with a couple of complex elements:
    - a class definition which will be referenced in the function
        closure (ClosuredClass)
    - an instance of this class, whose state determins the funciton
        behaviour (the field x=42)
    """

    class ClosuredClass:
        def __init__(self, x: int) -> None:
            self.x = x

    closured_obj = ClosuredClass(x=42)

    def closured(param: str) -> int:
        return len(param) + closured_obj.x

    return closured


def _run_worker():
    import os

    os.system("python -m qless.worker")


def _start_workers(n_workers: int) -> List[Process]:
    processes = []
    for worker in range(n_workers):
        p = Process(target=_run_worker)
        p.start()
        processes.append(p)
    return processes


def _start_local_postgres_docker_db() -> str:
    db_url = "postgres://postgres:test@localhost:5000/qless"
    os.system("docker kill pg-test")
    os.system(
        "docker run --rm --name pg-test -e POSTGRES_PASSWORD=test -d -p 5000:5432 postgres:11"
    )
    while not "database system is ready" in os.popen("docker logs pg-test").read():
        log.log("Waiting for DB to be ready...")
        sleep(0.2)
    sleep(0.5)
    return db_url


if __name__ == "__main__":
    run_test_e2e()
