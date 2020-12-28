from prefect import task, Flow

@task
def get_message():
    return 'Hello, world!'

def test_get_message():
    with Flow('test') as flow:
        task1 = get_message()
    state = flow.run()
    assert state.result[task1].result == 'Hello, world!'
