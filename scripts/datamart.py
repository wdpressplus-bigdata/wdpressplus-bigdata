import pandas as pd
import sqlalchemy
from pyhive import presto

query = '''
SELECT date_trunc('day', timestamp) time,
       wbanno,
       avg(temperature) avg_temperature
FROM uscrn GROUP BY 1, 2
'''
conn = presto.connect(host='presto-server', port=8080)
df = pd.read_sql_query(query, conn, parse_dates=['timestamp'])
print(df.head(2))

uri = "postgresql://datamart:datamart@datamart:5432/datamart"
engine = sqlalchemy.create_engine(uri)
df.to_sql('uscrn_summary', index=False, if_exists='replace', con=engine)
print('uscrn_summary created')
