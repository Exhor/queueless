
from dataclasses import dataclass
from enum import Enum


class TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    ERROR = 3
    DONE = 4


@dataclass
class TaskSummary:
    id_: int
    owner: int
    status: TaskStatus


@dataclass
class TaskDetails:
    id_: int
    func: str
    kwargs: str
    results: str
