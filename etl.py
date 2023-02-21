import json
import requests
import pandas as pd
from datetime import datetime
from prefect import task, flow


@task(name="Extract Task", description="Makes request.", retries=10, retry_delay_seconds=10)
def extract(url: str) -> dict:
    res = requests.get(url, verify=False)
    if not res:
        raise Exception('URL seems unavailable!')
    print('good on extract')
    return json.loads(res.content)

@task(name="Transform Task", description="Transform Existing Data.")
def transform(data: dict) -> pd.DataFrame:
    transformed = []
    for user in data:
        transformed.append({
            'ID': user['id'],
            'UserId': user['userId'],
            'Title': user['title']
        })
    print('good on transform')
    return pd.DataFrame(transformed)

@task(name="Load Task", description="Persists data to disk.")
def load(data: pd.DataFrame, path: str) -> None:
    print('loading to csv...')
    data.to_csv(path_or_buf=path, index=False)
    print('good on csv')

@flow(name="ETL Flow", log_prints=True)
def etl_flow(p_url):
    users = extract(url=p_url)
    df_users = transform(users)
    load(data=df_users, path=f'data/posts_{int(datetime.now().timestamp())}.csv')
    return flow

if __name__ == '__main__':
    flow = etl_flow(p_url='https://jsonplaceholder.typicode.com/posts')