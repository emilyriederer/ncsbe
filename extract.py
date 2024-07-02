import shutil
from importlib import resources
from ncsbe import hist, resu, abev, vofi
from ncsbe.util import ELECTION_DATES
import os

# SETUP DIRS IF NOT EXIST
for d in ['hist','vofi','abev','resu']:
    if not os.path.exists(f'raw/{d}'):
        os.makedirs(f'raw/{d}')
    if not os.path.exists(f'raw/{d}/zip'):
        os.makedirs(f'raw/{d}/zip')

# DOWNLOAD COMPRESSED FILES
hist.hist_download()
for e in ELECTION_DATES:
    vofi.vofi_download(e)
    abev.abev_download(e)
    resu.resu_download(e)

# HISTORICAL VOTING RECORDS
hist.hist_expand()
for e in ELECTION_DATES:
    hist.hist_convert_pq(e)
shutil.rmtree('raw/hist/stg')

# PRECINCT-LEVEL RESULTS 
for e in ELECTION_DATES:
    resu.resu_expand(e)
    resu.resu_convert_pq(e)
    shutil.rmtree('raw/resu/stg')

# VOTERFILE SNAPSHOTS
for e in ELECTION_DATES: 
    vofi.vofi_expand(e)
    vofi.vofi_convert_pq(e)
    shutil.rmtree('raw/vofi/stg')

# ABSENTEE / EARLY VOTE 
for e in ELECTION_DATES:
    abev.abev_expand(e)
    abev.abev_convert_pq(e)
    shutil.rmtree('raw/abev/stg')

# COUNTY LOOKUP TABLE
path_lkup = 'raw/lkup'
file_lkup = "county_lookup.csv"

if not os.path.exists(path_lkup): 
    os.makedirs(path_lkup)
with resources.path("ncsbe", file_lkup) as f:
    shutil.copyfile(f, f"{path_lkup}/{file_lkup}")