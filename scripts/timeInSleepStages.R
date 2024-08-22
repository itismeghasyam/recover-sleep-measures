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
    "SleepLevelRestless",
    "SleepLevelAsleep"
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
    across(starts_with("SleepLevel"), as.numeric),
    PercentDeep = SleepLevelDeep/(SleepLevelDeep + SleepLevelLight + SleepLevelRem),
    PercentLight = SleepLevelLight/(SleepLevelDeep + SleepLevelLight + SleepLevelRem),
    PercentRem = SleepLevelRem/(SleepLevelDeep + SleepLevelLight + SleepLevelRem),
    PercentRestless = SleepLevelRestless/(SleepLevelAsleep + SleepLevelRestless)
  )

calculate_stats <- function(data, variable) {
  data %>%
    summarise(
      across(all_of({{variable}}), list(
        Mean = ~mean(.x, na.rm = TRUE),
        Median = ~median(.x, na.rm = TRUE),
        Variance = ~var(.x, na.rm = TRUE),
        Percentile5 = ~quantile(.x, 0.05, na.rm = TRUE),
        Percentile95 = ~quantile(.x, 0.95, na.rm = TRUE),
        Count = ~n()
      ), .names = "{.col}_{.fn}"),
      .groups = "drop"
    )
}

# Weekly statistics
weekly_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>%
  calculate_stats("PercentDeep") %>% 
  ungroup()

# All-time statistics
alltime_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier) %>%
  calculate_stats("PercentDeep") %>%
  ungroup()
