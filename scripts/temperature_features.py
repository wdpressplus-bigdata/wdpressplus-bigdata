import pandas as pd
import sqlalchemy
from pyhive import presto

query = '''
SELECT wbanno,
       min(temperature) t_min,
       avg(temperature) t_avg,
       max(temperature) t_max
FROM uscrn GROUP BY 1
'''
conn = presto.connect(host='presto-server', port=8080)
features = pd.read_sql_query(query, conn)
print(features)

uri = "postgresql://datamart:datamart@datamart:5432/datamart"
engine = sqlalchemy.create_engine(uri)
features.to_sql('temperature_features', index=False, if_exists='replace', con=engine)
print('temperature_features created')
