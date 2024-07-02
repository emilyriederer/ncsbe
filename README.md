# ncsbe

This repo contains code to clean and pre-process historic North Carolina election data from 2014-2022. 

This data is derived from publicly available files from the North Carolina State Board of Elections. Full documentation is on [their website](https://www.ncsbe.gov/results-data).

## Data Domains 

The following data domains are created in this repository.

- **Historical Voter-Level Turnout** (`hist`): One record for each voting-event, identified by a voter's `id_voter` and the `dt_election`
- **Absentee Ballot and Early Vote Activity** (`abev`): One record for each voting-event for in-person early voting or mail voting, identified by a voter's `id_voter` and the `dt_election`. Records are a subset of those in `hist` but include additional early-vote specific fields
- **Voterfile Snapshots**: Voterfile snapshots as-of each election date, identified by a voter's `id_voter`
  + **Eligible Voters** (`vofi`): Active, inactive, and temporary (military and overseas) voterfile records
  + **Ineligible Voters** (`novo`): Denied and removed voterfile records
- **Precinct & Contest-Level Results** (`resu`): Votes in each contest, with one record for each election date, precinct, and candidate

## Output Structure

## Code Structure



## Credits

`ncsbe` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `py-pkgs-cookiecutter` [template](https://github.com/py-pkgs/py-pkgs-cookiecutter).
