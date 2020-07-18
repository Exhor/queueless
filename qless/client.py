from qless.task import Task, TaskStatus
from qless.sql import session_scope

def submit(func: Callable[..., Any], owner: int) -> int:
    task = Task(id_=-1, status=TaskStatus.PENDING, owner=owner, func=func_str, kwargs=kwargs_str, results="")
    with session_scope() as session:
        pass
