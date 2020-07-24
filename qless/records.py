from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.types import Integer, Text, DateTime

BASE = declarative_base()


class TaskRecord(BASE):  # type: ignore

    __tablename__ = "task"

    id_ = Column(Integer, primary_key=True)
    creator = Column(Integer, nullable=False)
    owner = Column(Integer, nullable=False)
    status = Column(Integer, nullable=False)  # see TaskStatus

    function_dill = Column(Text, nullable=False)
    kwargs_dill = Column(Text, nullable=False)
    results_dill = Column(Text, nullable=False)
    retries = Column(Integer, nullable=False)
    last_updated = Column(DateTime, nullable=False)
    requires_tag = Column(Text, nullable=False)


class WorkerRecord(BASE):  # type: ignore

    __tablename__ = "worker"

    id_ = Column(Integer, primary_key=True)
    last_heartbeat = Column(DateTime, nullable=False)
    working_on_task_id = Column(Integer, nullable=True)
    tag = Column(Text, nullable=False)
