import sys
from datetime import datetime, timedelta
from multiprocessing import Process
from time import sleep
from typing import Optional, Any, List

import dill

from queueless import sql
from queueless.log import log
from queueless.records import TaskRecord, WorkerRecord
from queueless.task import TaskStatus, Task, NO_OWNER


def start_local_workers(
    n_workers: int, db_url: str, worker_tag: str = "", cleanup_timeout: float = 300
) -> List[Process]:
    """
    Starts queueless workers using forked processes

    :param n_workers: how many workers to start
    :param db_url: database connection string
    :param worker_tag: an arbitrary string to associated with all workers, later can
        be used to limit which workers can run which tasks
    :param cleanup_timeout: how often (on average, it is random) to perform 'cleanup'
        queueless does not have a Scheduler or Master process. All workers perform
        maintenance operations such as removing old tasks, resetting stuck tasks, etc.

    :return: a list of Process objects pointing to the started processes containing
        each worker
    """
    processes = []
    for worker in range(n_workers):
        p = Process(
            # target=_run_worker_via_cli, kwargs={"db_url": db_url, "worker_tag": worker_tag}, daemon=True
            target=_run_worker,
            kwargs={
                "db_url": db_url,
                "worker_tag": worker_tag,
                "cleanup_timeout": cleanup_timeout,
            },
            daemon=True,
        )
        p.start()
        processes.append(p)
    return processes


def _run_worker(
    db_url: str,
    tick_seconds: float = 1,
    worker_tag: str = "",
    cleanup_timeout: float = 300,
) -> None:
    """ Infinite loop that continuously monitors the DB for tasks,
    claims tasks, executes their code, and saves results.

    It will also periodically do a clean up (reset orphaned tasks, delete old results)

    :param db_url: full database url string, including credentials, e.g.
        postgres://postgres:test@localhost:5000/qless
    :param tick_seconds: worker will sleep for this long between polls to the DB for
        tasks and cleanup attempts.
    :param worker_tag: enables this worker to execute tasks with this tag
    :param cleanup_timeout: when performing cleanup, any worker which has not reported
        a heartbeat in `cleanup_timeout` seconds will be considered dead, and its
        tasks reset to PENDING
    """
    sql.startup(db_url)
    me = _register_worker(worker_tag)
    log(f"Worker started. Tag: {worker_tag}. Id = {me}")
    while _hearbeat(me):
        _cleanup(cleanup_timeout)
        sleep(tick_seconds)
        task = _claim_task(me, worker_tag)
        if task is not None:
            _run_task(task, me)


def _set_worker_task_to_none(worker_id: int):
    """ Setting a worker task to None means the worker is not considered to be
    working on any tasks any more. This has no effect on the worker behaviour, its
    purpose is only to stop cleanups from resetting a task if the worker is considered
    non-responsive (dead)
    """
    with sql.session_scope() as session:
        worker = session.query(WorkerRecord).get(worker_id)
        worker.working_on_task_id = None
        session.merge(worker)


def _register_worker(worker_tag: str) -> int:
    """ Registers the worker with the DB and returns its unique id """
    with sql.session_scope() as session:
        record = WorkerRecord(last_heartbeat=datetime.now(), tag=worker_tag)
        session.add(record)
        session.flush()
        return int(record.id_)


def _hearbeat(worker_id: int) -> bool:
    """ Interacts with the Worker table, checking if worker needs to die, and
    updating the worker heartbeat to signal it is still alive

    :returns: True if all is normal, False if execution should stop
    """
    with sql.session_scope() as session:
        record = session.query(WorkerRecord).get(worker_id)
        if record is None:
            return False
        record.last_heartbeat = datetime.now()  # TODO: only if diff > 3 sec
        session.merge(record)
    return True


def _cleanup(cleanup_timeout: float):
    """ Perform routine maintenance jobs such as erasing old records, resetting
    dead worker's tasks, etc.
    """
    _search_for_dead_workers_and_disown_their_tasks(cleanup_timeout)


def _search_for_dead_workers_and_disown_their_tasks(cleanup_timeout: float) -> None:
    """ If a worker is dead, the task it was working on should be reset, so another
    worker can pick it up.

    :param cleanup_timeout: a worker will be considered dead if it hasnt updated its
        heartbeat on the DB in the last `cleanup_timeout` seconds
    """
    too_long_ago = datetime.now() - timedelta(seconds=cleanup_timeout)
    with sql.session_scope() as session:
        # Find any workers whose last heartbeat was more than `cleanup_timeout` ago
        dead_workers = (
            session.query(WorkerRecord)
            .with_for_update()
            .filter(WorkerRecord.last_heartbeat < too_long_ago)
            .all()
        )
        for dead_worker in dead_workers:
            task_id = dead_worker.working_on_task_id
            if task_id is not None:
                # Reset the task (if any) that the worker was working on
                orphan_task = session.query(TaskRecord).with_for_update().get(task_id)
                log(
                    f"Worker {dead_worker.id_} has not responded in {cleanup_timeout} "
                    f"seconds. Its task {task_id} will be disowned, and..."
                )
                orphan_task.owner = NO_OWNER

                retries = orphan_task.retries
                if retries == 0:
                    log(f"...Task Status set to TIMEOUT. No more retries left")
                    orphan_task.status = TaskStatus.TIMEOUT.value
                else:
                    log(f"...Task status set to PENDING. {retries} retries left")
                    orphan_task.status = TaskStatus.PENDING.value
                    orphan_task.retries = retries - 1
                session.merge(orphan_task)

                dead_worker.working_on_task_id = None


def _serialise(obj: Any) -> str:
    return str(dill.dumps(obj))


def _deserialise(serialised: str) -> Any:
    return dill.loads(eval(serialised))


def _run_task(task: Task, worker_id: int) -> None:
    """ Execute the task function and save its result if it completes or the exception
    if it errors

    :param task: the task to execute
    :param worker_id: the id of the current worker, running the task. This is needed
        as a worker id is compared against the DB to check the worker still owns the
        task, to avoid multiple workers working on the same task
    """
    func = _deserialise(task.func)
    params = _deserialise(task.kwargs)

    args = str(params)[:20]
    log(
        f"Starting task {task.id_}. Function: {func.__name__}. Args: {args}. Worker: {worker_id}"
    )

    try:
        _save_results(task.id_, func(**params), worker_id, TaskStatus.DONE)
        log(f"Task {task.id_} completed successfully")
    except Exception as err:
        _save_results(task.id_, err, worker_id, TaskStatus.ERROR)
        log(f"Error while running task {task.id_}: {err}")
    finally:
        _set_worker_task_to_none(worker_id)


def _save_results(
    task_id: int, results: Any, worker_id: int, status: Optional[TaskStatus] = None
) -> None:
    """ Saves the result for a given task (either the return value of the function
    executed or the exception raised).

    :param task_id: unique identifier for the task. This is created when the task is
        submitted.
    :param results: any object that can be serialised
    :param worker_id: the identifier for the worker attempting to save the results. This
        is needed because only workers that legitimally own a task are allowed to save
        results for it. This is to prevent multiple workers working on the same task
    :param status: if set, the task status will also be updated to this
    """
    serialised_results = _serialise(results)
    with sql.session_scope() as session:
        task = session.query(TaskRecord).get(task_id)

        # Only save results if tasks is RUNNING and this worker still owns it
        if task.status == TaskStatus.RUNNING.value and task.owner == worker_id:
            log(
                f"Saving result for task {task_id}, with size = {len(serialised_results)}"
            )
            task.results_dill = serialised_results
            if status is not None:
                task.status = status.value
            session.merge(task)
        elif task.status != TaskStatus.RUNNING.value:
            log(f"Task {task_id} not RUNNING. Status={task.status}. Results discarded.")
        elif task.owner != worker_id:
            log(
                f"Worker {worker_id} running task {task_id}, but task owner is: {task.owner}. Results discarded."
            )

        worker = session.query(WorkerRecord).get(worker_id)
        worker.working_on_task_id = None
        session.merge(worker)


def _claim_task(worker_id: int, worker_tag: str = "") -> Optional[Task]:
    """ Grabs a PENDING task from the DB and marks it as owned by this worker, and set
     is to RUNNING

    :param worker_id: identity of the worker that is claiming the task. Both the worker
        record and the task record will be modified, to have references to one another
    :param worker_tag: an arbitrary string, if set, only tasks with this tag will be
        claimed.

    :return: either the task claimed or None if no suitable (PENDING and correct tag)
        tasks were found
    """
    with sql.session_scope() as session:
        rec = (
            session.query(TaskRecord)
            .with_for_update()
            .filter_by(status=TaskStatus.PENDING.value, owner=NO_OWNER)
            .filter(TaskRecord.requires_tag.in_([worker_tag, ""]))
            .first()
        )
        if rec is None:
            return None
        rec.owner = worker_id
        rec.status = TaskStatus.RUNNING.value
        session.merge(rec)
        func = rec.function_dill
        kwargs = rec.kwargs_dill
        id_ = rec.id_
        creator = rec.creator

        worker = session.query(WorkerRecord).get(worker_id)
        worker.working_on_task_id = id_
        session.merge(worker)

    return Task(
        id_=id_,
        owner=worker_id,
        creator=creator,
        status=TaskStatus.RUNNING.value,
        func=func,
        kwargs=kwargs,
        results="",
    )


def _help() -> str:
    return """
Usage: 
    $ python -m queueless.worker POSTGRES_DB_URL [TAG] 

    POSTGRES_DB_URL: full postgres connection string
    TAG: arbitrary string which enables this worker to execute tasks with the same tag
    
Example:
    $ python -m queueless.worker postgres://postgres:test@localhost:5000/qless my_tag_1
"""


if __name__ == "__main__":
    args = sys.argv
    if len(args) < 2:
        log(_help())
    else:
        db_connection_string = args[1]
        tag = args[2] if len(args) > 2 else ""
        tick = float(args[3]) if len(args) > 3 else 1.0
        _run_worker(db_connection_string, tick, tag)
