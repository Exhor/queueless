import os

from tests.services import start_local_postgres_docker_db

print("Start dockerised postgres")
db_url = start_local_postgres_docker_db()

print("Build")
os.system("docker build -t pip_install_test .")
