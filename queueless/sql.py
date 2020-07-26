from contextlib import contextmanager
from typing import Generator

from sqlalchemy import MetaData
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session, sessionmaker

from queueless.log import log
from queueless.records import BASE

_engine = None
_session_maker = None


def startup(db) -> None:
    """ Call once to start the DB engine, and populate all tables

    Creates a global sql Engine. as is typical in SQLAlchemy
    ref: https://docs.sqlalchemy.org/en/13/core/connections.html
    """
    global _engine, _session_maker
    if _engine is None:
        _engine = create_engine(db)
        _session_maker = sessionmaker(bind=_engine)
        _make_qless_db_if_not_present(db)
        _create_all_tables()


def reset() -> None:
    """ Destroy all task data. Drops all tables and recreates them """
    BASE.metadata.drop_all(_engine)
    log("Dropped all tables")
    _create_all_tables()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """ Gives a context in which to perform database
    operations safely. Autocommits on exit. Rolls back on any errors.

    Example:
        with session_scope() as session:
            session.query(...)


    """
    session = _session_maker()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _make_qless_db_if_not_present(db):
    # We need an engine without the `qless` db name
    # NB: Autocommit is required to create databases
    engine = create_engine(db.replace("qless", ""), isolation_level="AUTOCOMMIT")

    databases = engine.execute("SELECT datname FROM pg_database;").fetchall()
    databases = [d[0] for d in databases]
    if "qless" not in databases:
        conn = engine.connect()
        conn.execute("CREATE DATABASE qless")
        conn.close()
        log("Created database '/qless'")


def _create_all_tables() -> None:
    global _engine
    if "task" not in MetaData().tables:
        BASE.metadata.create_all(_engine)
        log("Created all tables")
