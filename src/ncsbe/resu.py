import os
import shutil
from importlib import resources
import polars as pl
import polars.selectors as cs
from ncsbe.util import validate_transform_date, download, subset_rename

def resu_meta(election_date: str):

    dt = validate_transform_date(election_date)
    meta = {
        'dt': dt,
        'name_in': f"results_pct_{dt}",
        'name_out': f"results_pct_{dt}",
        'path_zip': 'raw/resu/zip',
        'path_stg': 'raw/resu/stg',
        'path_out': f"raw/resu/year={dt[0:4]}"
    }
    return meta

def resu_download(election_date: str):

    meta = resu_meta(election_date)
    name_in = meta.get('name_in')  
    path_zip = meta.get('path_zip')  

    url = f'https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/{election_date}/{name_in}.zip'
    download(url, path_zip)

def resu_expand(election_date: str):

    meta = resu_meta(election_date)
    name_in = meta.get('name_in')  
    path_zip = meta.get('path_zip')  
    path_stg = meta.get('path_stg')

    shutil.unpack_archive(f'{path_zip}/{name_in}.zip', path_stg)

def resu_convert_pq(election_date: str):

    meta = resu_meta(election_date)
    name_out = meta.get('name_out')
    path_stg = meta.get('path_stg')
    path_out = meta.get('path_out')
    
    if not os.path.exists(path_out):
        os.makedirs(path_out)

    with resources.path("ncsbe", "county_lookup.csv") as f:
        df_lkup = pl.scan_csv(f)

    df = pl.scan_csv(f'{path_stg}/{name_out}.txt', 
                     separator = '\t', 
                     infer_schema_length = 0)
    
    (
        df
        .select(pl.all().name.map(lambda c: c.lower().replace(' ','_')))
        .pipe( subset_rename, type = 'resu' )
        .with_columns( cs.string().str.strip_chars() )
        .with_columns( 
            cs.starts_with('dt_').replace('', None).str.strptime(pl.Date, '%m/%d/%Y'),
            cs.starts_with('n_').cast(pl.Int64)
        )
        .join(df_lkup, on = 'desc_county', how = 'left')
        .drop('desc_county')
        .unique()
        .sink_parquet(f'{path_out}/resu_{election_date.replace("_","")}.parquet')
    )