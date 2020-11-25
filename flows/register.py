from os.path import abspath
from prefect.engine.results import S3Result
from prefect.engine.executors.dask import LocalDaskExecutor
from prefect.environments.execution.local import LocalEnvironment
from prefect.environments.storage import Docker
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock


# download

from flows.download import flow

flow.storage = Docker(
    python_dependencies=[
        'boto3',
        'pandas',
    ],
    env_vars={
        'PREFECT__CONTEXT__MINIO_URL': 'http://minio:9000',
    },
)

flow.environment = LocalEnvironment(executor=LocalDaskExecutor())

flow.result = S3Result(
    bucket='prefect',
    boto3_kwargs={
        'endpoint_url': 'http://minio:9000',
        'aws_access_key_id': 'accesskey',
        'aws_secret_access_key': 'secretkey',
    },
)

flow.schedule = Schedule(clocks=[CronClock("0 0 * * *")])

flow.register(project_name="My Project", labels=['docker'])


# warehouse

from flows.warehouse import flow

flow.storage = Docker(
    base_image='k24d/spark:latest',
    files={
      abspath('scripts/warehouse.py'): '/opt/scripts/warehouse.py',
    },
)

flow.register(project_name="My Project", labels=['docker'])


# datamart

from flows.datamart import flow

flow.storage = Docker(
    base_image='k24d/python:latest',
    python_dependencies=[
      'pandas',
      'pyhive',
      'psycopg2',
      'sqlalchemy',
      'pyyaml',
    ],
)

flow.register(project_name="My Project", labels=['docker'])
