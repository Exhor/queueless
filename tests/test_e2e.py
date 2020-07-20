from multiprocessing import Process
from time import sleep
from typing import Callable

from qless import client, sql, log


def make_func() -> Callable[[str], int]:
    class ClosuredClass:
        def __init__(self, x: int) -> None:
            self.x = x

    closured_obj = ClosuredClass(x=42)

    def closured(param: str) -> int:
        return len(param) + closured_obj.x

    return closured


def run_worker():
    import os

    os.system("python -m qless.worker")


def start_workers(n_workers: int) -> Process:
    p = Process(target=run_worker)
    p.start()
    return p


def test_db_url() -> str:
    return "postgres://postgres:test@localhost:5000/qless"


def start_db() -> str:
    db_url = test_db_url()
    os.system("docker kill pg-test")
    os.system(
        "docker run --rm --name pg-test -e POSTGRES_PASSWORD=test -d -p 5000:5432 postgres:11"
    )
    while not "database system is ready" in os.popen("docker logs pg-test").read():
        log.log("Waiting for DB to be ready...")
        sleep(0.2)
    return db_url


if __name__ == "__main__":
    db_url = start_db()
    sql.start_global_engine(db_url)
    # sql.reset()
    w = start_workers(1)
    func = make_func()
    task_id = client.submit(func, {"param": "abc"}, 123)
    sleep(3)
    result = client.get_task_result(task_id)
    assert result == len("abc") + 42
    log.log("All OK!")
