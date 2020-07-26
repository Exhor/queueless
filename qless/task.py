from enum import Enum

NO_OWNER = 0


class TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    ERROR = 3
    DONE = 4
    TIMEOUT = 5


class Task:
    def __init__(
        self,
        id_: int,
        owner: int,
        creator: int,
        status: TaskStatus,
        func: str,
        kwargs: str,
        results: str,
        retries: int = 0,
    ):
        self.id_ = id_
        self.owner = owner
        self.creator = creator
        self.status = status
        self.func = func
        self.kwargs = kwargs
        self.results = results
        self.retries = retries
