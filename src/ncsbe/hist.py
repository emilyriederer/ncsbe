import polars as pl
import os
import shutil
from ncsbe.util import ELECTION_DATES, download, subset_rename

def hist_download(): 

    url = 'https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis_Statewide.zip'
    download(url, 'raw/hist/zip')

def hist_expand():

    shutil.unpack_archive('raw/hist/zip/ncvhis_Statewide.zip', 
                          'raw/hist/stg')

def hist_convert_pq(election_date: str):

    df = pl.scan_csv('raw/hist/stg/ncvhis_Statewide.txt', 
                     separator = '\t', 
                     infer_schema_length = 0)

    path_out = f"raw/hist/year={election_date[0:4]}"
    if not os.path.exists(path_out):
        os.makedirs(path_out)
        
    (
    df
    .pipe( subset_rename, type = 'hist' )
    .with_columns( pl.col('dt_election').str.strptime(pl.Date, '%m/%d/%Y') )
    .filter( pl.col('dt_election').dt.to_string('%Y_%m_%d') == pl.lit(election_date) )
    .unique()
    .sink_parquet(f'{path_out}/hist_{election_date.replace("_","")}.parquet')
    )
