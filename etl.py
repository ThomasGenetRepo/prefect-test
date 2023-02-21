import pandas as pd
from prefect import task, flow
import numpy as np

@task(name='create_data')
def create_data():
    l1 = [i for i in range(0,10, 1)]
    l2 = [i for i in range(0,20, 2)]
    l3 = [i for i in range(0,30, 3)]
    return pd.DataFrame({
        'A': l1,
        'B': l2,
        'C': l3
    })
    
@task(name='transform')
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df.A = df.A.map(lambda x: np.sin(x))
    df.B = df.B.map(lambda x: np.cos(x))
    return df
    
@task(name = 'download')
def save_df(df: pd.DataFrame):
    df.to_csv('./my_data.csv', index=False)

@task(name = 'read file')
def read_data() -> pd.DataFrame:
    return pd.read_csv('./my_data.csv')

@task(name = 'final battle')
def final_battle(df: pd.DataFrame) -> pd.DataFrame:
    df['D'] = df.A + df.B + df.C 
    print(df)
    return df

@flow(log_prints=True)
def my_flow():
    data = create_data()
    tdf = transform(data)
    save_df(tdf)
    df = read_data()
    final_df = final_battle(df)

if __name__ == "__main__":
   my_flow()