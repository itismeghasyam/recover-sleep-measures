source("scripts/etl/fetch-data.R")

fitbit_sleeplogs <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogs$"))
  )

fitbit_sleeplogdetails <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogdetails"))
  )

vars <- 
  selected_vars %>% 
  filter(str_detect(Export, "sleeplogs$")) %>% 
  pull(Variable)

# Load the desired subset of the dataset in memory and do some feature 
# engineering for derived variables
sleeplogs_df <- 
  fitbit_sleeplogs %>% 
  select(all_of(c(vars, "LogId"))) %>% 
  collect() %>% 
  distinct() %>% 
  mutate(
    Date = lubridate::as_date(StartDate),
    Duration = as.numeric(Duration),
    Efficiency = as.numeric(Efficiency),
    IsMainSleep = as.logical(IsMainSleep),
    SleepStartTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, StartDate, NA)),
    SleepEndTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, EndDate, NA)),
    MidSleep = format((lubridate::as_datetime(SleepStartTime) + ((Duration/1000)/2)), format = "%H:%M:%S"),
    SleepStartTime = 
      ((format(SleepStartTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
    SleepEndTime = 
      ((format(SleepEndTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
    MidSleep = 24*lubridate::hms(MidSleep)/lubridate::hours(24)
  )

numEpisodes <-
  sleeplogs_df %>%
  select(ParticipantIdentifier, StartDate) %>%
  mutate(Date = lubridate::as_date(StartDate)) %>%
  select(-StartDate) %>%
  count(ParticipantIdentifier, Date,
        name = "NumEpisodes")

# Use the sleeplogdetails dataset to derive Number of Awakenings, REM Onset 
# Latency, and REM Fragmentation Index variables
sleeplogdetails_vars <- 
  selected_vars %>% 
  filter(str_detect(Export, "sleeplogdetails$")) %>% 
  pull(Variable)

sleeplogdetails_df <- 
  fitbit_sleeplogdetails %>% 
  select(all_of(sleeplogdetails_vars)) %>% 
  collect() %>% 
  distinct() %>% 
  left_join(y = (sleeplogs_df %>% select(ParticipantIdentifier, LogId, IsMainSleep)), 
            by = join_by("LogId", "ParticipantIdentifier")) %>% 
  group_by(ParticipantIdentifier, LogId, id) %>% 
  arrange(StartDate, .by_group = TRUE) %>% 
  ungroup()

sleeplogdetails_df_unique <- sleeplogdetails_df[!duplicated(sleeplogdetails_df),]
rm(sleeplogdetails_df)

numawakenings_logid_filtered <- 
  sleeplogdetails_df_unique %>% 
  filter(IsMainSleep==TRUE) %>% 
  filter(if_any(c(StartDate, EndDate, Value, Type), ~ . != "")) %>% 
  select(ParticipantIdentifier, LogId, id, StartDate, Value) %>%
  group_by(ParticipantIdentifier, LogId, id) %>% 
  arrange(StartDate, .by_group = TRUE) %>% 
  filter(!(row_number()==n() & Value %in% c("wake", "awake"))) %>% 
  mutate(nextValue = dplyr::lead(Value, 1)) %>% 
  filter(nextValue %in% c("wake", "awake")) %>% 
  filter(!(Value %in% c("wake", "awake"))) %>% 
  summarise(
    NumAwakenings = dplyr::n(), 
    .groups = "drop"
  ) %>% 
  select(ParticipantIdentifier, LogId, NumAwakenings)

regex_wake <- stringr::regex("wake|awake", ignore_case = TRUE)
regex_rem <- stringr::regex("rem", ignore_case = TRUE)
rem_onset_latency <- 
  sleeplogdetails_df_unique %>% 
  filter(IsMainSleep==TRUE) %>% 
  filter(if_any(c(StartDate, EndDate, Value, Type), ~ . != "")) %>% 
  select(ParticipantIdentifier, LogId, id, StartDate, Value) %>% 
  group_by(ParticipantIdentifier, LogId, id) %>% 
  arrange(StartDate, .by_group = TRUE) %>% 
  mutate(
    firstNonWake = 
      ifelse(
        stringr::str_detect(Value,  regex_wake, negate = TRUE) & 
          cumsum(stringr::str_detect(Value,  regex_wake, negate = TRUE))==1, 
        TRUE, 
        FALSE
      ),
    firstREM = 
      ifelse(
        stringr::str_detect(Value, regex_rem) & 
          cumsum(stringr::str_detect(Value, regex_rem))==1, 
        TRUE, 
        FALSE
      )
  ) %>% 
  filter(firstNonWake | firstREM) %>% 
  summarise(
    remOnsetLatency = 
      difftime(
        StartDate[firstREM] %>% first() %>% lubridate::ymd_hms(), 
        StartDate[firstNonWake] %>% first() %>% lubridate::ymd_hms(),
        units = "secs"
      ) %>% 
      as.numeric(),
    .groups = "drop"
  ) %>% 
  ungroup() %>% 
  select(ParticipantIdentifier, LogId, remOnsetLatency)

rem_fragmentation_index <- 
  sleeplogdetails_df_unique %>% 
  filter(IsMainSleep==TRUE) %>% 
  filter(if_any(c(StartDate, EndDate, Value, Type), ~ . != "")) %>% 
  group_by(ParticipantIdentifier, LogId, id) %>% 
  arrange(StartDate, .by_group = TRUE) %>% 
  mutate(prevValue = dplyr::lag(Value, 1)) %>% 
  filter(prevValue == "rem" & Value != "rem") %>%
  summarise(remTransitions = n(), .groups = "drop") %>% 
  left_join(y = (sleeplogs_df %>% select(ParticipantIdentifier, LogId, SleepLevelRem)), 
            by = join_by("ParticipantIdentifier", "LogId")) %>% 
  mutate(SleepLevelRem = as.numeric(SleepLevelRem)) %>% 
  filter(SleepLevelRem > 0) %>%
  mutate(remFragmentationIndex = remTransitions/(SleepLevelRem/60)) %>% 
  select(ParticipantIdentifier, LogId, remFragmentationIndex) %>% 
  ungroup()

# Merge the original data frame with the new data frames for derived variables,
# joining by ParticipantIdentifier and LogId
df_joined <- 
  sleeplogs_df %>% 
  select(
    c(ParticipantIdentifier, 
      LogId, 
      StartDate, 
      IsMainSleep,
      SleepStartTime, 
      SleepEndTime, 
      MidSleep, 
      Efficiency
      )
  ) %>% 
  left_join(y = numawakenings_logid_filtered, 
            by = join_by("ParticipantIdentifier", "LogId")) %>%
  left_join(y = rem_onset_latency, 
            by = join_by("ParticipantIdentifier", "LogId")) %>%
  left_join(y = rem_fragmentation_index, 
            by = join_by("ParticipantIdentifier", "LogId")) %>% 
  mutate(Date = lubridate::as_date(StartDate))


# Reduce each combination of ParticipantIdentifier and Date to a single record
# by returning the average value of variables for any groups containing more
# than 1 record
reduced_to_daily_df <- 
  df_joined %>% 
  group_by(ParticipantIdentifier, Date) %>% 
  summarise(
    SleepStartTime = psych::circadian.mean(SleepStartTime, na.rm = TRUE),
    SleepEndTime = psych::circadian.mean(SleepEndTime, na.rm = TRUE),
    MidSleep = psych::circadian.mean(MidSleep, na.rm = TRUE),
    Efficiency = mean(Efficiency, na.rm = TRUE),
    NumAwakenings = mean(NumAwakenings, na.rm = TRUE),
    remOnsetLatency = as.numeric(mean(remOnsetLatency, na.rm = TRUE)),
    remFragmentationIndex = mean(remFragmentationIndex, na.rm = TRUE),
    .groups = "drop"
  ) %>% 
  mutate(across(where(is.numeric), ~na_if(., NaN)))

# Join the data frame containing Number of Episodes with the daily records 
# reduced data frame
output <- 
  reduced_to_daily_df %>% 
  left_join(y = numEpisodes, 
            by = join_by("ParticipantIdentifier", "Date")) %>% 
  relocate(NumEpisodes, .after = NumAwakenings) %>% 
  mutate(Day = lubridate::wday(Date, label = TRUE, abbr = FALSE, week_start = 7)) %>% 
  relocate(Day, .after = Date)

write_csv(
  x = output, 
  file = file.path(outputDataDir, "daily_sleep_measures.csv")
)

f <- 
  synapser::synStore(
    synapser::File(
      path = str_subset(list.files(outputDataDir, full.names = T), 
                 "daily_sleep_measures.csv"),
      parent = dailyMeasuresSynDirId
    ), 
    executed = "https://github.com/Sage-Bionetworks/recover-sleep-measures/blob/main/scripts/daily-measures.R",
    used = parquetDirId
  )
