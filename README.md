# queueless
Python distributed task execution using a DB instead of queues.

# Install 
```
$ pip install queueless
```

# Quickstart
queueless uses postgres to communicate. You must have a postgres db running, e.g.
````bash
docker run --rm --name pg-test -e POSTGRES_PASSWORD=test -it -p 5000:5432 postgres:11
````

On another terminal, start a worker, pointing it to the database:
```bash
$ python -m queueless.worker postgres://postgres:test@localhost:5000/qless
```
Now in your script
```python
from queueless import client
import time

def function(x: int) -> float:
    return x + 1

# send the task
task_id = client.sumbit(func=function, kwargs={"x": 42}, owner=123)

# wait a bit
time.sleep(2)

# get the result
assert client.get_task_result(task_id) == 42 + 1
```
## Scaling up
Simply run more workers on new terminals.

## Using docker 
You can also run the worker with docker
### Build
```bash
$ python build.py

$ docker run -it --network host --entrypoint python qless qless/worker.py postgres://postgres:test@localhost:5000/qless
```
---
# Development setup
## test (requires docker)
python tests/test_e2e.py

## get poetry
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

## check you're in the right virtual env
poetry env info

## update dependencies
poetry update