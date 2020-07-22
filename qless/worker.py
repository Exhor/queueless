from random import random
from time import sleep
from typing import Optional
from uuid import uuid4

import dill

from qless import sql
from qless.log import log
from qless.task import TaskStatus, Task
from qless.task_record import TaskRecord


def work_loop(cleanup_every_seconds: int = 1000, tick_seconds: float = 0.01) -> None:
    me = hash(str(uuid4())) % 1_000_000_000
    while True:
        sleep(tick_seconds)
        task = claim_task(me)
        if task is not None:
            run(task)
        if random() < 1 / (cleanup_every_seconds / tick_seconds):
            cleanup()


def cleanup():
    retry_task_whose_owner_is_dead()


def retry_task_whose_owner_is_dead():
    pass


def run(task: Task) -> None:
    func = dill.loads(eval(task.func))
    params = dill.loads(eval(task.kwargs))

    args = str(params)[:20]
    log(f"Starting task {task.id_}. Function: {func.__name__}. Args: {args}")

    try:
        results = str(dill.dumps(func(**params)))
        status = TaskStatus.DONE
    except Exception as err:
        status = TaskStatus.ERROR
        results = str(err)
    save(task.id_, results, status, task.owner)


def save(task_id: int, results: str, status: TaskStatus, owner: int) -> None:
    with sql.session_scope() as session:
        rec = session.query(TaskRecord).get(task_id)

        # Task no longer owned? Or stopped by another process? Do not save
        if rec.owner != owner or rec.status != TaskStatus.RUNNING.value:
            return None

        rec.results_dill = results
        rec.status = status.value
        session.merge(rec)


def claim_task(owner: int) -> Optional[Task]:
    no_owner = 0
    search_status = TaskStatus.PENDING.value
    new_status = TaskStatus.RUNNING.value
    with sql.session_scope() as session:
        rec = (
            session.query(TaskRecord)
            .filter_by(status=search_status, owner=no_owner)
            .first()
        )
        if rec is None:
            return None
        rec.owner = owner
        rec.status = new_status
        session.merge(rec)
        func = rec.function_dill
        kwargs = rec.kwargs_dill
        id_ = rec.id_
        creator = rec.creator

    return Task(
        id_=id_,
        owner=owner,
        creator=creator,
        status=new_status,
        func=func,
        kwargs=kwargs,
        results="",
    )


if __name__ == "__main__":
    sql.startup("postgres://postgres:test@localhost:5000/qless")
    work_loop()
