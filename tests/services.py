import os
from time import sleep

from queueless import log


def start_local_postgres_docker_db() -> str:
    db_url = "postgres://postgres:test@localhost:5000/qless"
    os.system("docker kill pg-test")
    assert not os.system(
        "docker run --rm --name pg-test -e POSTGRES_PASSWORD=test -d -p 5000:5432 postgres:11"
    )
    while not "database system is ready" in os.popen("docker logs pg-test").read():
        log.log("Waiting for DB to be ready...")
        sleep(0.2)
    sleep(0.5)
    return db_url
