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
- Generate summaries for derived sleep measures (as of Oct 2024, these are Sleep Start Time, Sleep End Time, Mid-sleep, Sleep Efficiency, Number of Awakenings, Number of Sleep Episodes, REM Sleep Fragmentation Index, and REM Onset Latency) on a daily timescale

For all other timescales, run the following scripts individually to generate their outputs:
- [percentSleepStartIn8pm2am.R](scripts/)
- [sleepSD.R](scripts/sleepSD.R)
- [sri.R](scripts/sri.R)
- [timeInSleepStages.R](timeInSleepStages.R)

All five of the main sleep measure scripts above is self-contained in that they each run the [fetch-data.R](scripts/etl/fetch-data.R) ETL script in the beginning and egress code (write data results to CSV and store in Synapse) at the end of their scripts.
