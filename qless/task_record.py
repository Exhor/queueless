from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.types import Integer, Text

BASE = declarative_base()


class TaskSummaryRecord(BASE):  # type: ignore
    """ Holds task status, owner. Small records, fast access """

    __tablename__ = "task_summary"

    id_ = Column(Integer, primary_key=True)
    owner = Column(Integer, nullable=False)
    status = Column(Integer, nullable=False)  # 1-4: pending, running, done, error


class TaskDetailsRecord(BASE):  # type: ignore
    """ Task specification and results, only accessed seldom """

    __tablename__ = "task_details"

    id_ = Column(Integer, primary_key=True)
    function_dill = Column(Text, nullable=False)
    kwargs_dill = Column(Text, nullable=False)
    results_dill = Column(Text, nullable=False)
