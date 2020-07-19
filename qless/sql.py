from contextlib import contextmanager
from typing import Generator

from sqlalchemy import MetaData, inspect

from qless.log import log
from qless.task_record import BASE
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session, sessionmaker

_engine = None
_session_maker = None

def start_global_engine(db) -> None:
    global _engine, _session_maker
    if _engine is None:
        _engine = create_engine(db)
        _session_maker = sessionmaker(bind=_engine)
        make_qless_db_and_tables(db)

def make_qless_db_and_tables(db) -> None:
    eng = create_engine(db[:db.rfind("/")])
    databases = eng.execute('SELECT datname FROM pg_database;').fetchall()
    databases = [d[0] for d in databases]
    if "qless" not in databases:
        conn=eng.connect()
        conn.execute("commit")
        conn.execute("CREATE DATABASE qless")
        conn.close()
        log("Created database '/qless'")
    _create_all_tables()

def _create_all_tables() -> None:
    global _engine
    BASE.metadata.create_all(_engine)
    log("Created all tables")


def reset() -> None:
    BASE.metadata.drop_all(_engine)
    log("Dropped all tables")
    _create_all_tables()

@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session = _session_maker()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
