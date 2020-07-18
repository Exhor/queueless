
from dataclasses import dataclass
from enum import Enum


class TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    ERROR = 3
    DONE = 4


@dataclass
class Task:
    id_: int
    status: TaskStatus
    owner: int
    func: str
    kwargs: str
    results: str
