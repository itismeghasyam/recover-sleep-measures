source("scripts/etl/fetch-data.R")

fitbit_sleeplogs <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogs$"))
  )

vars <- 
  c("ParticipantIdentifier", 
    "LogId",
    "IsMainSleep",
    "StartDate", # YYYY-MM-DDTHH:MM:SS format
    "EndDate", # YYYY-MM-DDTHH:MM:SS format,
    "SleepLevelDeep", 
    "SleepLevelLight", 
    "SleepLevelRem",
    "MinutesAsleep"
  )

# Load desired subset of the data in memory and do some feature engineering
sleeplogs_df <- 
  fitbit_sleeplogs %>% 
  select(all_of(c(vars))) %>% 
  collect() %>% 
  distinct() %>% 
  mutate(
    Date = lubridate::as_date(StartDate), # YYYY-MM-DD format
    IsMainSleep = as.logical(IsMainSleep),
    MinutesAsleep = as.numeric(MinutesAsleep),
    across(starts_with("SleepLevel"), as.numeric),
    PercentDeep = SleepLevelDeep/(MinutesAsleep),
    PercentLight = SleepLevelLight/(MinutesAsleep),
    PercentRem = SleepLevelRem/(MinutesAsleep)
  )

# Weekly statistics
weekly_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>%
  summarise(
    across(.cols = all_of(c("PercentDeep", "PercentLight", "PercentRem")), 
           .fns = 
             list(
               Mean = ~mean(.x, na.rm = TRUE),
               Median = ~median(.x, na.rm = TRUE),
               Variance = ~var(.x, na.rm = TRUE),
               Percentile5 = ~quantile(.x, 0.05, na.rm = TRUE),
               Percentile95 = ~quantile(.x, 0.95, na.rm = TRUE),
               Count = ~sum(!is.na(.x))
             ),
           .names = "{.col}_{.fn}"),
    .groups = "drop"
  ) %>% 
  ungroup()

# All-time statistics
alltime_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier) %>%
  summarise(
    across(.cols = all_of(c("PercentDeep", "PercentLight", "PercentRem")), 
           .fns = 
             list(
               Mean = ~mean(.x, na.rm = TRUE),
               Median = ~median(.x, na.rm = TRUE),
               Variance = ~var(.x, na.rm = TRUE),
               Percentile5 = ~quantile(.x, 0.05, na.rm = TRUE),
               Percentile95 = ~quantile(.x, 0.95, na.rm = TRUE),
               Count = ~sum(!is.na(.x))
             ),
           .names = "{.col}_{.fn}"),
    .groups = "drop"
  ) %>% 
  ungroup()

write_csv(
  x = weekly_stats, 
  file = file.path(outputDataDirTimeInSleepStages, "weekly_stats_time_in_sleep_stages.csv")
)

write_csv(
  x = alltime_stats, 
  file = file.path(outputDataDirTimeInSleepStages, "alltime_stats_time_in_sleep_stages.csv")
)

manifest_path <- file.path(outputDataDirTimeInSleepStages, "output-data-manifest.tsv")

synapserutils::generate_sync_manifest(
  directory_path = outputDataDirTimeInSleepStages,
  parent_id = timeInSleepStagesSynDirId,
  manifest_path = manifest_path
)

manifest <- read_tsv(manifest_path)

thisScriptUrl <- "https://github.com/Sage-Bionetworks/recover-sleep-measures/blob/main/scripts/timeInSleepStages.R"

manifest <-
  manifest %>%
  mutate(executed = thisScriptUrl, 
         used = parquetDirId)

write_tsv(manifest, manifest_path)

synclient <- reticulate::import("synapseclient")
syn_temp <- synclient$Synapse()
syn_temp$login()

synutils <- reticulate::import("synapseutils")
synutils$syncToSynapse(syn = syn_temp, manifestFile = manifest_path)
