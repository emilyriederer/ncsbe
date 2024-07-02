import requests
import shutil
from importlib import resources
import polars as pl
import polars.selectors as cs

ELECTION_DATES = [
  '2022_11_08',
  '2022_05_17',
  '2020_11_03',
  '2020_03_03',
  '2018_11_06',
  '2018_05_08',
  '2016_11_08',
  '2016_03_15',
  '2014_11_04',
  '2014_05_06',
]

def validate_transform_date(election_date: str):

    if election_date not in ELECTION_DATES:
        raise Exception(f"{election_date} is not a valid election date")
    
    return election_date.replace("_","")

def download(url: str, dir: str):

    local_filename = f'{dir}/{url.split('/')[-1]}'
    with requests.get(url, stream=True) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    return local_filename

def subset_rename(df: pl.DataFrame, type: str):

    with resources.path("ncsbe.columns", f"{type}.txt") as f:
        df_cols = pl.read_csv(f).filter( 
            pl.col('from').is_in(df.columns)
            )

    cols_fr = df_cols.get_column('from').to_list()
    cols_to = df_cols.get_column('to').to_list()
    rename_map = dict(zip(cols_fr, cols_to))

    df_out = ( 
        df
        .select(cs.by_name( cols_fr ))
        .rename( rename_map )
    )

    return df_out