from prefect import task, Flow

@task
def get_message():
    return 'Hello, world!'

def test_get_message():
    with Flow('test') as flow:
        get_message()
    state = flow.run()
    task = flow.get_tasks(name='get_message')[0]
    assert state.result[task].result == 'Hello, world!'
