from enum import Enum

from pydantic import BaseModel


class Base(BaseModel):
    pass


class TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    ERROR = 3
    DONE = 4


class Task(Base):
    id_: int
    owner: int
    creator: int
    status: TaskStatus
    func: str
    kwargs: str
    results: str
    retries: int = 0
