from time import sleep
from typing import Optional
from uuid import uuid4

import dill

from qless.sql import session_scope
from qless.task import Task, TaskStatus
from qless.task_record import TaskDetailsRecord, TaskSummaryRecord


def work_loop() -> None:
    me = hash(str(uuid4()))
    while True:
        sleep(0.01)
        task = claim_task(me)
        if task is None:
            continue
        run(task)


def run(task: Task) -> None:
    func = dill.loads(eval(task.func))
    params = dill.loads(eval(task.kwargs))
    try:
        results = str(dill.dumps(func(params)))
        status = TaskStatus.DONE
    except Exception as err:
        status = TaskStatus.ERROR
        results = str(err)
    save(task, results, status)


def save(task_id: int, results: str, status: TaskStatus) -> None:
    with session_scope() as session:
        rec = session.query(TaskDetailsRecord).get(task_id).one()
        rec.results = results
        session.merge(rec)
    with session_scope() as session:
        rec = session.query(TaskStatus).get(task_id).one()
        rec.status = status.value
        session.merge(rec)


def claim_task(owner: int) -> Optional[Task]:
    with session_scope() as session:
        rec = session.query(TaskSummaryRecord).filter_by(status=TaskStatus.PENDING).first()
        if rec is None or rec.owner != 0:
            return None
        rec.owner = owner
        session.merge(rec)
        rec_id = rec.id

    # Separate session. Minimise lock time on Task Status table
    with session_scope() as session:
        details = session.query(TaskDetailsRecord).get(rec_id).one()
        func = details.function_dill
        kwargs = details.kwargs_dill
    return Task(
        id_=rec_id, status=TaskStatus.PENDING, owner=owner, func=func, kwargs=kwargs, results=""
    )

