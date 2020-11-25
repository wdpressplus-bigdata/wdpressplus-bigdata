import prefect
from prefect import Flow
from prefect.tasks.prefect import StartFlowRun


a = StartFlowRun(flow_name='download', project_name='My Project', wait=True)
b = StartFlowRun(flow_name='warehouse', project_name='My Project', wait=True)
c = StartFlowRun(flow_name='datamart', project_name='My Project', wait=True)


with Flow('daily_batch') as flow:
    a.set_downstream(b)
    b.set_downstream(c)


if __name__ == '__main__':
    flow.run()
