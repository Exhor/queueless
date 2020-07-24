from datetime import datetime
from typing import Any, Callable, Dict

import dill

from qless.records import TaskRecord
from qless.sql import session_scope
from qless.task import TaskStatus


def submit(
    func: Callable[..., Any], kwargs: Dict[str, Any], creator: int, requires_tag: str
) -> int:
    """ Sends the function to be executed remotely, with the given kwargs

    :param creator: a unique identifier for the creator of this task, to ease later
        queries such as 'get tasks for this creator'

    :param requires_tag: only workers that have this tag will pick up this
        task. Defaults to '' (any worker)
    """
    func_str = str(dill.dumps(func))
    kwargs_str = str(dill.dumps(kwargs))
    status = TaskStatus.PENDING.value

    rec = TaskRecord(
        creator=creator,
        owner=0,
        status=status,
        function_dill=func_str,
        kwargs_dill=kwargs_str,
        results_dill="",
        retries=0,
        last_updated=datetime.now(),
        requires_tag=requires_tag,
    )
    with session_scope() as session:
        session.add(rec)
        session.flush()
        task_id = rec.id_

    return task_id


def get_task_status(task_id: int) -> TaskStatus:
    with session_scope() as session:
        return session.query(TaskRecord).get(task_id).status


def get_task_result(task_id: int) -> Any:
    with session_scope() as session:
        rec = session.query(TaskRecord).get(task_id)
        results = rec.results_dill
    return dill.loads(eval(results)) if results else None


def get_task_retries(task_id: int) -> int:
    with session_scope() as session:
        return session.query(TaskRecord).get(task_id).retries
