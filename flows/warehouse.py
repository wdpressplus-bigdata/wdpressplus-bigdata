import prefect
from prefect import task, Flow
from prefect.tasks.shell import ShellTask
from prefect.tasks.templates.strings import StringFormatter

spark_submit_command = StringFormatter(template='''
spark-submit --packages org.apache.hadoop:hadoop-aws:3.2.0 \
  /opt/scripts/warehouse.py {yesterday}
''')

bash = ShellTask(log_stderr=True, return_all=True)


with Flow('warehouse') as flow:
    bash(command=spark_submit_command())


if __name__ == '__main__':
    flow.run()
