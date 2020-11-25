import glob
import yaml

import prefect
from prefect import task, Flow


@task
def presto_query(query):
    import pandas as pd
    from pyhive import presto
    conn = presto.connect(host='presto-server', port=8080)
    return pd.read_sql_query(query, conn, parse_dates=['timestamp'])


@task
def datamart_replace(df, table):
    import sqlalchemy
    uri = "postgresql://datamart:datamart@datamart:5432/datamart"
    engine = sqlalchemy.create_engine(uri)
    df.to_sql(table, index=False, if_exists='replace', con=engine)


with Flow('datamart') as flow:
    for path in glob.glob('queries/*.yml'):
        params = yaml.safe_load(open(path))
        df = presto_query(params['query'])
        datamart_replace(df, params['table'])


if __name__ == '__main__':
    flow.run()
