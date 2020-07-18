from qless.sql import session_scope
import dill
from contextlib import contextmanager
from dataclasses import dataclass
from time import sleep
from typing import Generator, Optional
from uuid import uuid4

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session, sessionmaker

from qless.task import Task, TaskDetails, TaskStatus


def work_loop() -> None:
    me = hash(str(uuid4()))
    while True:
        sleep(0.01)
        task = claim_task(me)
        if task is None:
            continue
        run(task)


def run(task: Task) -> None:
    func = dill.loads(task.func)
    params = dill.loads(task.kwargs)
    try:
        results = str(dill.dumps(func(params)))
        status = TaskStatus.DONE
    except Exception as err:
        status = TaskStatus.ERROR
        results = str(err)
    save(task, results, status)


def save(task: Task, results: str, status: TaskStatus) -> None:
    with session_scope() as session:
        rec = session.query(TaskDetails).get(Task.id_).one()
        rec.results = results
        session.merge(rec)
    with session_scope() as session:
        rec = session.query(TaskStatus).get(Task.id_).one()
        rec.status = status.value
        session.merge(rec)


def claim_task(owner: int) -> Optional[Task]:
    with session_scope() as session:
        rec = session.query(TaskStatus).filter_by(status=TaskStatus.PENDING).first()
        if rec is None or rec.owner != 0:
            return None
        rec.owner = owner
        session.merge(rec)
        rec_id = rec.id

    # Separate session. Minimise lock time on TaskStatus table
    with session_scope() as session:
        details = session.query(TaskDetails).get(rec_id).one()
        func = details.function_dill
        kwargs = details.kwargs_dill
    return Task(
        id_=rec_id, status=TaskStatus.PENDING, owner=owner, func=func, kwargs=kwargs, results=""
    )

