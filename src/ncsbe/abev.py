import re
import os
import shutil
from importlib import resources
import polars as pl
import polars.selectors as cs
from ncsbe.util import validate_transform_date, download, subset_rename

def abev_meta(election_date: str):

    dt = validate_transform_date(election_date)
    meta = {
        'dt': dt,
        'name_in': f"absentee_{dt}",
        'name_out': f"abev_{dt}",
        'path_zip': 'raw/abev/zip',
        'path_stg': 'raw/abev/stg',
        'path_out': f"raw/abev/year={dt[0:4]}",
        'ext': 'csv' if dt != '20140506' else 'txt',
        'sep': ',' if dt != '20140506' else '\t'
    }
    return meta

def abev_download(election_date: str):

    meta = abev_meta(election_date)
    name_in = meta.get('name_in')  
    path_zip = meta.get('path_zip')

    url = f'https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/{election_date}/{name_in}.zip' 
    download(url, path_zip)

def abev_expand(election_date: str):

    meta = abev_meta(election_date)
    name_in = meta.get('name_in') 
    name_out = meta.get('name_out') 
    path_zip = meta.get('path_zip')  
    path_stg = meta.get('path_stg')
    ext = meta.get('ext')

    shutil.unpack_archive(f'{path_zip}/{name_in}.zip', path_stg)

    # handle unescaped quotations
    BLOCKSIZE = 1048576
    with open(f'{path_stg}/{name_in}.{ext}', 'r', encoding = 'latin-1') as src:
        with open(f'{path_stg}/{name_out}.{ext}', 'w', encoding = 'utf8') as dst:
            while True:
                contents = src.read(BLOCKSIZE)
                if not contents:
                    break
                contents = re.sub(r'(\w+)"+(\w+)' , r"\1 \2", contents)
                dst.write(contents)

def abev_convert_pq(election_date: str):

    meta = abev_meta(election_date)
    name_out = meta.get('name_out')
    path_stg = meta.get('path_stg')
    path_out = meta.get('path_out')
    ext = meta.get('ext')
    sep = meta.get('sep')

    if not os.path.exists(path_out):
        os.makedirs(path_out)

    with resources.path("ncsbe", "county_lookup.csv") as f:
        df_lkup = pl.scan_csv(f)

    df = pl.scan_csv(f'{path_stg}/{name_out}.{ext}', 
                     separator = sep, 
                     infer_schema_length = 0)
    
    (
      df
      .with_columns( cs.string().str.strip_chars() )
      .with_columns( cs.string().replace(['','\x00'], [None, None]))
      .with_columns(        
          cs.ends_with('_dt').replace('', None).str.strptime(pl.Date, '%m/%d/%Y'),
          pl.col('age').cast(pl.Int16)
      )
      .pipe( subset_rename, type = 'abev' )
      .join(df_lkup, on = 'desc_county', how = 'left')
      .drop('desc_county')
      .unique()
      .sink_parquet(f'{path_out}/{name_out}.parquet')
    )
