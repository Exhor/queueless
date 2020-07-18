from contextlib import contextmanager
from typing import Generator

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session, sessionmaker
from sqlalchemy import Table, Column, Integer, String, MetaData

from qless.task_record import BASE, TaskDetailsRecord, TaskSummaryRecord

engine = None
session_maker = None

def start_global_engine(db) -> None:
    global engine, session_maker
    if engine is None:
        engine = create_engine(db)
        session_maker = sessionmaker(bind=engine)

    if "task_details" not in BASE.metadata.tables:
        conn=engine.connect()
        conn.execute("commit")
        conn.execute("CREATE DATABASE qless")
        conn.close()
        BASE.metadata.create_all(engine)


def reset() -> None:
    BASE.metadata.drop_all(engine)
    BASE.metadata.create_all(engine)


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session = session_maker()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()