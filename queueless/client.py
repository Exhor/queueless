from datetime import datetime
from typing import Any, Callable, Dict

import dill

from queueless import sql
from queueless.records import TaskRecord
from queueless.sql import session_scope
from queueless.task import TaskStatus, NO_OWNER


def startup(db_url: str) -> None:
    """ Call once per python session. Prepares sql, engine, tables and sessions """
    sql.startup(db_url)


def submit(
    func: Callable[..., Any],
    kwargs: Dict[str, Any],
    creator: int,
    requires_tag: str = "",
    n_retries_if_worker_hangs: int = 1,
) -> int:
    """ Sends the function to be executed remotely, with the given kwargs

    :param creator: identifier for the creator of this task, so that it can later
        be filtered/retrieved
    :param requires_tag: if specified, only workers started with this tag will be
        allowed to run this task
    :param func: the function to run
    :param kwargs: the keyword arguments to pass to the function
    :param creator: a unique identifier for the creator of this task, to ease later
        queries such as 'get tasks for this creator'
    :param requires_tag: only workers that have this tag will pick up this
        task. Defaults to '' (any worker)
    :param n_retries_if_worker_hangs: how many times should this task be retried if it
        makes the workers hang. queueless reassign tasks from non-responsive workers
        to new workers. If a task takes too long, or has resource problems, it may be
        its fault that the worker executing it died. This number limits the chances a
        task has to complete before it is marked as TIMEOUT
    :return: a unique identifier for the task, which can later be used to query its
        status or get the results
    """

    func_str = str(dill.dumps(func))
    kwargs_str = str(dill.dumps(kwargs))
    status = TaskStatus.PENDING.value

    rec = TaskRecord(
        creator=creator,
        owner=NO_OWNER,
        status=status,
        function_dill=func_str,
        kwargs_dill=kwargs_str,
        results_dill="",
        retries=n_retries_if_worker_hangs,
        last_updated=datetime.now(),
        requires_tag=requires_tag,
    )
    with session_scope() as session:
        session.add(rec)
        session.flush()
        task_id = rec.id_

    return task_id


def get_task_status(task_id: int) -> TaskStatus:
    """ A task status represents what part of the execution journey it is in,
    possible journeys:

    PENDING -> RUNNING -> DONE  (use get_task_result() to get the result)

    PENDING -> RUNNING -> ERROR  (use get_task_result() to get the error)

    PENDING -> RUNNING -> PENDING -> RUNNING -> TIMEOUT  (task takes too long or hangs)

    """
    with session_scope() as session:
        return TaskStatus(session.query(TaskRecord).get(task_id).status)


def get_task_result(task_id: int) -> Any:
    """ Returns the result of running the task, which can

    :param task_id: unique identifier for the task, you get this number when you call
        submit().

    :return: either be the return value of the function submitted (if status is DONE,
    or the exception raised (if status is ERROR)
    """
    with session_scope() as session:
        rec = session.query(TaskRecord).get(task_id)
        results = rec.results_dill
    return dill.loads(eval(results)) if results else None
