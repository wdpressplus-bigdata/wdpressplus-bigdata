import boto3
import datetime
import pandas as pd
import requests

import prefect
from prefect import task, Flow
from prefect.engine.executors.dask import LocalDaskExecutor


@task
def get_filenames():
    year = prefect.context.yesterday[:4]
    url = 'https://www1.ncdc.noaa.gov/pub/data/uscrn/products/stations.tsv'
    # url = 'https://github.com/wdpressplus-bigdata/uscrn/raw/main/stations.tsv'
    df = pd.read_csv(url, delimiter='\t', header=None)
    result = []
    for _, row in df.loc[df[12]=='Operational'].iterrows():
        state = row[2]
        location = row[3].replace(' ', '_')
        vector = row[4].replace(' ', '_')
        result.append(f"CRNS0101-05-{year}-{state}_{location}_{vector}.txt")
    return result[:10]


@task(max_retries=1, retry_delay=datetime.timedelta(seconds=60))
def download_file(filename):
    year = prefect.context.yesterday[:4]
    prefix = 'https://www1.ncdc.noaa.gov/pub/data/uscrn/products/subhourly01'
    # prefix = 'https://github.com/wdpressplus-bigdata/uscrn/raw/main'
    r = requests.get(f"{prefix}/{year}/{filename}", stream=True)
    r.raise_for_status()
    minio_params = {
        'endpoint_url': prefect.context.minio_url,
        'aws_access_key_id': 'accesskey',
        'aws_secret_access_key': 'secretkey',
    }
    s3 = boto3.Session().client('s3', **minio_params)
    bucket_name = 'datalake'
    object_name = f"uscrn/{prefect.context.yesterday}/{filename}.gz"
    s3.upload_fileobj(r.raw, bucket_name, object_name)
    prefect.context.logger.info(f"uploaded to s3://{bucket_name}/{object_name}")
    return f"s3://{bucket_name}/{object_name}"


with Flow('download') as flow:
    filenames = get_filenames()
    download_file.map(filenames)


if __name__ == '__main__':
    with prefect.context(minio_url='http://localhost:9000'):
        flow.run(executor=LocalDaskExecutor())
