from pyspark.sql import SparkSession, Row
import sqlalchemy
import sys

date = sys.argv[1]

spark = SparkSession.builder.getOrCreate()

def parse_line(line):
    f = line.split()
    precipitation = None if f[9] == '-9999.0' else float(f[9])
    return Row(wbanno=f[0], date=f[1], precipitation=precipitation)

rdd = spark.sparkContext.textFile(f"s3a://datalake/uscrn/{date}/*")
df = rdd.map(parse_line).toDF()
df.createOrReplaceTempView('uscrn')

query = '''
SELECT wbanno,
       avg(precipitation) p_avg,
       max(precipitation) p_max
FROM (
  SELECT wbanno, date, sum(precipitation) precipitation
  FROM uscrn GROUP BY 1, 2
)
GROUP by 1
'''
features = spark.sql(query).toPandas()
print(features)

uri = "postgresql://datamart:datamart@datamart:5432/datamart"
engine = sqlalchemy.create_engine(uri)
features.to_sql('precipitation_features', index=False, if_exists='replace', con=engine)
print('precipitation_features created')
