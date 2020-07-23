# queueless
Python task distribution using a DB instead of queues

# Install 
Coming soon...

# Usage
## Requirements
You must have a postgres db running and accessible from a docker container

## Starting a worker

### Using docker (recommended)
Once you have the postgres string, pass it as a param to the worker, e.g.

`
$ docker run -it --network host --entrypoint python qless qless/worker.py postgres://postgres:test@localhost:5000/qless
`

### From python
You can also start a worker with just

```python
from qless import worker
worker.work_loop("postgres://<your postgres string>")  # blocking, infinite loop
```


)
## Using the client
```python
from qless import client
from time import sleep

# make a function
def my_func(x):
    return x + 1

# Submit a task
task_id = client.submit(my_func, {"x": 1}, 123)

# wait a bit
sleep(2) 

# get the result
print(client.get_task_result(task_id))  # = 2
```

# Development setup
## test (requires docker)
python tests/test_e2e.py

## get poetry
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

## check you're in the right virtual env
poetry env info

## update dependencies
poetry update