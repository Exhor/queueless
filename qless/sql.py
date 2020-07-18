import dill
from contextlib import contextmanager
from dataclasses import dataclass
from time import sleep
from typing import Generator
from uuid import uuid4

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session, sessionmaker

engine = None
session_maker = None

def get_engine(db):
    global engine, session_maker
    if engine is None:
        engine = create_engine(db)
        session_maker = sessionmaker(bind=engine)
    return engine

@contextmanager
def session_scope(db_url: str) -> Generator[Session, None, None]:
    session = session_maker()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()