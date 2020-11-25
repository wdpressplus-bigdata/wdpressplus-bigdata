from prefect import task, Flow

@task
def get_message():
    return 'Hello, world!'

@task
def print_message(msg):
    print(msg)

with Flow('hello') as flow:
    msg = get_message()
    print_message(msg)

if __name__ == '__main__':
    flow.run()
