from typing import Any, Callable, Dict

import dill
from qless.sql import session_scope
from qless.task import TaskStatus
from qless.task_record import TaskSummaryRecord, TaskDetailsRecord


def submit(func: Callable[..., Any], kwargs: Dict[str, Any], owner: int) -> int:
    func_str = str(dill.dumps(func))
    kwargs_str = str(dill.dumps(kwargs))
    with session_scope() as session:
        rec = TaskSummaryRecord(owner=owner, status=TaskStatus.PENDING.value)
        session.add(rec)
        session.flush()
        task_id = rec.id_

        details = TaskDetailsRecord(id_=task_id, function_dill=func_str, kwargs_dill=kwargs_str, results_dill="")
        session.merge(details)

    # task = Task(id_=-1, status=TaskStatus.PENDING, owner=owner, func=func_str, kwargs=kwargs_str, results="")
    return task_id


def get_task_status(task_id: int) -> TaskStatus:
    with session_scope() as session:
        rec = session.query(TaskSummaryRecord).get(task_id).first()
        status = rec.status
    return status


def get_task_result(task_id: int) -> Any:
    with session_scope() as session:
        rec = session.query(TaskDetailsRecord).get(task_id)
        results_dill = rec.results_dill
    return dill.loads(eval(results_dill)) if results_dill else None
