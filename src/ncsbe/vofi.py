import os
import shutil
import polars as pl
import polars.selectors as cs 
from ncsbe.util import validate_transform_date, download, subset_rename

def vofi_meta(election_date: str):

    dt = validate_transform_date(election_date)
    meta = {
        'dt': dt,
        'name_in': f'VR_Snapshot_{dt}',
        'name_out': f'vofi_{dt}',
        'path_zip': 'raw/vofi/zip',
        'path_stg': 'raw/vofi/stg',
        'path_out': f'raw/vofi/year={dt[0:4]}'
    }
    return meta

def vofi_download(election_date: str):

    meta = vofi_meta(election_date)
    name_in = meta.get('name_in')  
    path_zip = meta.get('path_zip')  

    url = f'https://s3.amazonaws.com/dl.ncsbe.gov/data/Snapshots/{name_in}.zip'
    download(url, path_zip)

def vofi_expand(election_date: str):

    meta = vofi_meta(election_date)
    name_in = meta.get('name_in')  
    name_out = meta.get('name_out')
    path_zip = meta.get('path_zip')  
    path_stg = meta.get('path_stg')

    shutil.unpack_archive(f'{path_zip}/{name_in}.zip', path_stg)

    BLOCKSIZE = 1048576
    with open(f'{path_stg}/{name_in}.txt', 'r', encoding = 'utf16') as src:
        with open(f'{path_stg}/{name_out}.txt', 'w', encoding = 'utf8') as dst:
            while True:
                contents = src.read(BLOCKSIZE)
                if not contents:
                    break
                dst.write(contents)

def vofi_convert_pq(election_date: str):

    meta = vofi_meta(election_date)
    name_out = meta.get('name_out')
    path_stg = meta.get('path_stg')
    path_out = meta.get('path_out')
    path_out_novo = path_out.replace('vofi','novo')
    name_out_novo = name_out.replace('vofi','novo')

    if not os.path.exists(path_out):
        os.makedirs(path_out)
    
    if not os.path.exists(path_out_novo):
        os.makedirs(path_out_novo)

    df = pl.scan_csv(
            f'{path_stg}/{name_out}.txt',
            separator = '\t',
            infer_schema_length = 0)
    
    to_int = ['cd_county','cd_dist_ushr','cd_dist_ncsen','cd_dist_nchr','age']

    # prep consistent data cleaning
    df_clean = (
      df
      .pipe( subset_rename, type = 'vofi' )
      .with_columns( cs.string().str.strip_chars() )
      # clean up empty strings
      .with_columns( cs.string().replace(['','\x00'], [None, None]))
      # clean up date fields
      .with_columns( cs.by_name(to_int).cast(pl.Int16))
      .with_columns( pl.col('dt_cancel').replace('1900-01-01', None))
      .with_columns( cs.starts_with('dt_').str.strptime(pl.Date, '%Y-%m-%d') )
      .unique()
    )

    # write active entries
    (
    df_clean
    .filter( pl.col('cd_status').is_in(['A','S','I']))
    .sink_parquet(f'{path_out}/{name_out}.parquet')
    )

    # write inactive entries
    (
    df_clean
    .filter( pl.col('cd_status').is_in(['R','D']))
    .sink_parquet(f'{path_out_novo}/{name_out_novo}.parquet')
    )
