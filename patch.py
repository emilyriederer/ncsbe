import polars as pl
import glob

# read in datasets
df_ncid = (
    pl.scan_parquet('raw/vofi/year=2014/vofi_*.parquet')
    .select('cd_county', 'dt_election', 'id_voter', 'id_voter_county')
    .with_columns( pl.col('cd_county').cast(pl.Int64))
    .unique()
    .collect()
)

# patch ncid in 2014 abev files
for f in glob.glob('raw/abev/year=2014/*.parquet'):

    with open(f, 'r') as src:
      df_abev = pl.read_parquet(f)
    
    if 'id_voter' not in df_abev.columns:
        (
        df_abev
        .drop('raw/abev/year')
        .join(df_ncid, 
              how = 'left',
              on = ['cd_county','dt_election','id_voter_county'])
        .write_parquet(f)
        )