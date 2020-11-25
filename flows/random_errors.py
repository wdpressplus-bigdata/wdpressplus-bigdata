from prefect import task, Flow
import random

@task
def random_error():
    if random.random() < 0.5:
        raise RuntimeError()

with Flow('random-errors') as flow:
    for _ in range(8):
        random_error()

flow.register(project_name='My Project')

flow.run_agent()
