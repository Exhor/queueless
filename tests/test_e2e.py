import os
from time import sleep
from typing import Any, Callable

from qless.client import submit, get_task_result
from qless.sql import start_global_engine, reset


def make_func() -> Callable[[str], int]:
    class ClosuredClass:
        def __init__(self, x: int) -> None:
            self.x = x
    closured_obj = ClosuredClass(x=42)
    
    def closured(param: str) -> int:
        return len(param) + closured_obj.x 

    return closured   


def start_workers(n_workers: int) -> None:
    pass

def start_db() -> str:
    db_url = "postgres://postgres:test@localhost:5000/qless"
    # os.system(
    #     "docker run --rm --name pg-test -e POSTGRES_PASSWORD=test -d -p 5000:5432 postgres:11"
    # )
    return db_url

if __name__ == "__main__":
    db_url = start_db()
    start_global_engine(db_url)
    reset()
    start_workers(2)
    func = make_func()
    task_id = submit(func, {"param": "abc"}, 123)
    sleep(3)
    result = get_task_result(task_id)
    assert result == len("abc") + 42
