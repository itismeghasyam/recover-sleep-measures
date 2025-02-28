# RECOVER Sleep Measures

## Purpose
This repository hosts code to generate derived sleep measures on various timescales:
- Daily
- Weekly
- Sliding 3 week window
- All time
- All time + N months post-infection

## Usage

The [config file](config.yml) contains the Synapse IDs and directory/file paths specifying where to fetch/write input and output data to, so please update the parameters in the config file as needed.

Within the [scripts directory](scripts/), the [etl folder](scripts/etl/) contains ETL scripts which are run at the beginning of each main sleep measure script, and the [functions folder](scripts/functions/) contains custom R functions used in some of the main sleep measure scripts.

For daily timescale summaries, run [daily-measures.R](scripts/daily-measures.R)
- Generates summaries for derived sleep measures on a daily timescale
- As of Oct 2024, the daily sleep measures are:
  - Sleep Start Time
  - Sleep End Time
  - Mid-sleep
  - Sleep Efficiency
  - Number of Awakenings
  - Number of Sleep Episodes
  - REM Sleep Fragmentation Index, and
  - REM Onset Latency

See [RMHDR-262](https://sagebionetworks.jira.com/browse/RMHDR-262) for further details

For all other timescales, run the following scripts individually to generate their outputs:
- [percentSleepStartIn8pm2am.R](scripts/)
- [sleepSD.R](scripts/sleepSD.R)*
  - See note below about infection dates data and Seven Bridges
- [sri.R](scripts/sri.R)*
  - See note below about infection dates data and Seven Bridges
- [timeInSleepStages.R](timeInSleepStages.R)

See [RMHDR-263](https://sagebionetworks.jira.com/browse/RMHDR-263) for further details

All five of the main sleep measure scripts above is self-contained in that they each run the [fetch-data.R](scripts/etl/fetch-data.R) ETL script in the beginning and egress code (write data results to CSV and store in Synapse) at the end of their scripts.

### Infection Dates Data and Seven Bridges
Currently, the [sleepSD.R](scripts/sleepSD.R) and [sri.R](scripts/sri.R) scripts need to be run in the Seven Bridges compute environment, since this is currently the only location where the infection dates data can be accessed. In the beginning of both scripts, the user will be required to specify a path to the 'visits' CSV file, which contains data on infection dates.
