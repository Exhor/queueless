from typing import Any, Callable, Dict

import dill
from qless.sql import session_scope
from qless.task import TaskStatus
from qless.task_record import TaskRecord


def submit(func: Callable[..., Any], kwargs: Dict[str, Any], creator: int) -> int:
    func_str = str(dill.dumps(func))
    kwargs_str = str(dill.dumps(kwargs))
    status = TaskStatus.PENDING.value

    rec = TaskRecord(creator=creator, owner=0, status=status, function_dill=func_str,
                     kwargs_dill=kwargs_str, results_dill="")
    with session_scope() as session:
        session.add(rec)
        session.flush()
        task_id = rec.id_

    # task = Task(id_=task_id, status=TaskStatus.PENDING, owner=owner, func=func_str, kwargs=kwargs_str, results="")
    return task_id


def get_task_status(task_id: int) -> TaskStatus:
    with session_scope() as session:
        rec = session.query(TaskRecord.status).get(task_id)
    return rec


def get_task_result(task_id: int) -> Any:
    with session_scope() as session:
        rec = session.query(TaskRecord).get(task_id)
        results = rec.results_dill
    return dill.loads(eval(results)) if results else None
