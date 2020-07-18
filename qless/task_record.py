from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.types import Integer, Text

BASE = declarative_base()


class TaskStatus(BASE):  # type: ignore
    """ Holds task status, owner. Small records, fast access """

    __tablename__ = "task_status"

    id = Column(Integer, primary_key=True)
    owner = Column(Integer, nullable=False)
    status = Column(Integer, nullable=False)  # 1-4: pending, running, done, error


class TaskDetails(BASE):  # type: ignore
    """ Task specification and results, only accessed seldom """

    __tablename__ = "task_details"

    id = Column(Integer, primary_key=True)
    function_dill = Column(Text, nullable=False)
    results_dill = Column(Text, nullable=False)
