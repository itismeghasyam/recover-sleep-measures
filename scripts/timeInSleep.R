fitbit_sleeplogs <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogs$"))
  )

fitbit_sleeplogdetails <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogdetails"))
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
    "MinutesAsleep")

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
    MinutesAsleep = as.numeric(MinutesAsleep),
    SleepStartTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, StartDate, NA)),
    SleepStartTime = ((format(SleepStartTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
    SleepEndTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, EndDate, NA)),
    SleepEndTime = ((format(SleepEndTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
    PercentDeep = if_else(MinutesAsleep==0, NA, SleepLevelDeep/MinutesAsleep),
    PercentLight = if_else(MinutesAsleep==0, NA, SleepLevelLight/MinutesAsleep),
    PercentRem = if_else(MinutesAsleep==0, NA, SleepLevelRem/MinutesAsleep),
    PercentRestless = if_else(MinutesAsleep==0, NA, SleepLevelRestless/MinutesAsleep)
  )

calculate_stats <- function(data) {
  data %>%
    summarise(
      MeanDeep = mean(PercentDeep, na.rm = TRUE),
      MedianDeep = median(PercentDeep, na.rm = TRUE),
      VarianceDeep = var(PercentDeep, na.rm = TRUE),
      Percentile5Deep = quantile(PercentDeep, 0.05, na.rm = TRUE),
      Percentile95Deep = quantile(PercentDeep, 0.95, na.rm = TRUE),
      CountDeep = n(),
      MeanLight = mean(PercentLight, na.rm = TRUE),
      MedianLight = median(PercentLight, na.rm = TRUE),
      VarianceLight = var(PercentLight, na.rm = TRUE),
      Percentile5Light = quantile(PercentLight, 0.05, na.rm = TRUE),
      Percentile95Light = quantile(PercentLight, 0.95, na.rm = TRUE),
      CountLight = n(),
      MeanRem = mean(PercentRem, na.rm = TRUE),
      MedianRem = median(PercentRem, na.rm = TRUE),
      VarianceRem = var(PercentRem, na.rm = TRUE),
      Percentile5Rem = quantile(PercentRem, 0.05, na.rm = TRUE),
      Percentile95Rem = quantile(PercentRem, 0.95, na.rm = TRUE),
      CountRem = n(),
      MeanRestless = mean(PercentRestless, na.rm = TRUE),
      MedianRestless = median(PercentRestless, na.rm = TRUE),
      VarianceRestless = var(PercentRestless, na.rm = TRUE),
      Percentile5Restless = quantile(PercentRestless, 0.05, na.rm = TRUE),
      Percentile95Restless = quantile(PercentRestless, 0.95, na.rm = TRUE),
      CountRestless = n()
    )
}

# Weekly statistics
weekly_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier, Week = floor_date(Date, "week")) %>%
  calculate_stats() %>%
  ungroup()

# All-time statistics
alltime_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier) %>%
  calculate_stats() %>%
  ungroup()
