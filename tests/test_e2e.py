import os
from datetime import datetime
from multiprocessing import Process
from time import sleep
from typing import Callable, List

from qless import client, sql, log

# A Script to test the library functionality end to end
from qless.records import TaskRecord
from qless.task import TaskStatus
from qless.worker import _run_worker


def run_test_e2e():  # TODO: multiple workers are picking up the same task (bug)
    db_url = _start_local_postgres_docker_db()
    sql.startup(db_url)
    worker_tag = "e2e_test_worker"
    _start_workers(n_workers=1, db_url=db_url, worker_tag=worker_tag)
    _start_workers(n_workers=1, db_url=db_url, worker_tag="cleaner", cleanup_timeout=1)
    _start_workers(n_workers=1, db_url=db_url, worker_tag="cleaner", cleanup_timeout=1)
    _start_workers(n_workers=1, db_url=db_url, worker_tag="cleaner", cleanup_timeout=1)
    _start_workers(n_workers=1, db_url=db_url, worker_tag="cleaner", cleanup_timeout=1)
    func = _make_test_function()

    # cleaner should be able to work on tasks normally
    task_id = client.submit(func, {"param": "abc"}, 123, requires_tag="cleaner")
    _wait_for_true(lambda: client.get_task_result(task_id) is not None)
    result = client.get_task_result(task_id)
    assert result == len("abc") + 42
    log.log("[OK] Tasks run")

    # Tasks are resheduled if their worker is not alive
    # start a task which takes longer than the expected heartbeat,
    # resulting in the worker being considered 'dead' and its task
    # rescheduled, this should happen n_retries times
    task_id = client.submit(_sleep, {"seconds": 5}, 123, n_retries_if_worker_hangs=2)
    _wait_for_true(lambda: client.get_task_status(task_id) == TaskStatus.ERROR)
    assert client.get_task_status(task_id) == TaskStatus.TIMEOUT
    log.log("[OK] Orphaned tasks rescheduled")

    log.log("[OK] All OK! :)")


def _set_task_owner(task_id: int, owner_id: int) -> None:
    with sql.session_scope() as s:
        record = s.query(TaskRecord).get(task_id)
        record.owner = owner_id
        s.merge(record)


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


def _run_worker_via_cli(db_url: str, worker_tag: str) -> None:
    os.system(f"python -m qless.worker {db_url} {worker_tag}")


def _start_workers(
    n_workers: int, db_url: str, worker_tag: str, cleanup_timeout: float = 300
) -> List[Process]:
    processes = []
    for worker in range(n_workers):
        p = Process(
            # target=_run_worker_via_cli, kwargs={"db_url": db_url, "worker_tag": worker_tag}, daemon=True
            target=_run_worker,
            kwargs={
                "postgres_db_url": db_url,
                "worker_tag": worker_tag,
                "cleanup_timeout": cleanup_timeout,
            },
            daemon=True,
        )
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
