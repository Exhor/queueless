import sys
from datetime import datetime, timedelta
from time import sleep
from typing import Optional

import dill

from qless import sql
from qless.log import log
from qless.records import TaskRecord, WorkerRecord
from qless.task import TaskStatus, Task


def work_loop(
    db_url: str,
    tick_seconds: float = 1,
    worker_tag: str = "",
) -> None:
    """ Infinite loop that continuously monitors the DB for tasks,
    claims tasks, executes their code, and saves results

    It will also periodically do a clean up (reset orphaned tasks, delete old results)
    """
    sql.startup(db_url)
    me = _register_worker(worker_tag)
    log(f"Worker started. Tag: {worker_tag}. Id = {me}")
    while _hearbeat(me):
        sleep(tick_seconds)
        print(f"{datetime.now()} [{worker_tag}]")
        task = claim_task(me, worker_tag)
        if task is not None:
            run(task)


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
    cleanup()
    with sql.session_scope() as session:
        record = session.query(WorkerRecord).get(worker_id)
        if record is None:
            return False
        record.last_heartbeat = datetime.now()  # TODO: only if diff > 3 sec
        session.merge(record)
    return True


def cleanup():
    retry_task_whose_owner_is_dead()


def _max_seconds_without_hearbeat():
    return 5


def retry_task_whose_owner_is_dead():
    too_long_ago = datetime.now() - timedelta(seconds=_max_seconds_without_hearbeat())
    with sql.session_scope() as session:
        dead_workers = session.query(WorkerRecord).filter(WorkerRecord.last_heartbeat < too_long_ago).all()
        tasks_ids_to_reset = [w.working_on_task_id for w in dead_workers]
        if tasks_ids_to_reset:
            session.query(TaskRecord).filter(TaskRecord.id_.in_(tasks_ids_to_reset)).update({"status": TaskStatus.PENDING.value})





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


def claim_task(owner: int, worker_tag: str) -> Optional[Task]:
    no_owner = 0
    with sql.session_scope() as session:
        rec = (
            session.query(TaskRecord)
            .filter_by(status=TaskStatus.PENDING.value, owner=no_owner)
            .filter(TaskRecord.requires_tag.in_([worker_tag, ""]))
            .first()
        )
        if rec is None:
            return None
        rec.owner = owner
        rec.status = TaskStatus.RUNNING.value
        session.merge(rec)
        func = rec.function_dill
        kwargs = rec.kwargs_dill
        id_ = rec.id_
        creator = rec.creator

    return Task(
        id_=id_,
        owner=owner,
        creator=creator,
        status=TaskStatus.RUNNING.value,
        func=func,
        kwargs=kwargs,
        results="",
    )


if __name__ == "__main__":
    args = sys.argv
    db_url = "postgres://postgres:test@localhost:5000/qless"
    if len(args) < 2:
        log(f"SQL connection string not specified. Using default: {db_url}")
    else:
        db_url = args[1]

    worker_tag = args[2] if len(args) > 2 else "worker"

    work_loop(db_url, worker_tag)
