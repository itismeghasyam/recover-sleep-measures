source("~/recover-sleep-summaries/scripts/fetch-data.R")

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

# Load the desired subset of this dataset in memory and do some feature engineering for derived variables
sleeplogs_df <- 
  fitbit_sleeplogs %>% 
  select(all_of(c(vars, "LogId"))) %>% 
  collect() %>% 
  distinct() %>% 
  mutate(
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
  select(ParticipantIdentifier, StartDate, LogId) %>% 
  mutate(StartDate = lubridate::as_date(StartDate),
         episode = ifelse(!is.na(LogId), 1, NA)) %>% 
  count(ParticipantIdentifier, StartDate, wt = episode, sort = TRUE)

# Use the sleeplogs_sleeplogdetails dataset to derive NumAwakenings, 
# REM Onset Latency, and REM Fragmentation Index variables
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

equality_check <- 
  all.equal(
    sleeplogdetails_df[!duplicated(sleeplogdetails_df),], 
    sleeplogdetails_df
  )

if (equality_check) {
  sleeplogdetails_df_unique <- sleeplogdetails_df
} else {
  sleeplogdetails_df_unique <- sleeplogdetails_df[!duplicated(sleeplogdetails_df),]
}

numawakenings_logid_filtered <- 
  sleeplogdetails_df_unique %>% 
  filter(IsMainSleep==TRUE) %>% 
  select(ParticipantIdentifier, LogId, id, Value) %>%
  group_by(ParticipantIdentifier, LogId, id) %>% 
  summarise(
    NumAwakenings = 
      sum(
        Value %in% c("wake", "awake") &
          !(row_number() == 1 & Value %in% c("wake", "awake")) &
          !(row_number() == n() & Value %in% c("wake", "awake"))
      ),
    .groups = "keep") %>% 
  ungroup() %>% 
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
      )
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
  summarise(remTransitions = n()) %>% 
  left_join(y = (sleeplogs_df %>% select(ParticipantIdentifier, LogId, SleepLevelRem)), 
            by = join_by("ParticipantIdentifier", "LogId")) %>% 
  mutate(SleepLevelRem = as.numeric(SleepLevelRem)) %>% 
  filter(SleepLevelRem > 0) %>%
  mutate(remFragmentationIndex = remTransitions/(SleepLevelRem/60)) %>% 
  select(ParticipantIdentifier, LogId, remFragmentationIndex) %>% 
  ungroup()

# Merge the original data frame with the new data frames for derived variables
df_joined <- 
  left_join(x = sleeplogs_df, y = numawakenings_logid_filtered, by = join_by("ParticipantIdentifier", "LogId")) %>%
  left_join(y = rem_onset_latency, by = join_by("ParticipantIdentifier", "LogId")) %>%
  left_join(y = rem_fragmentation_index, by = join_by("ParticipantIdentifier", "LogId")) %>% 
  select(
    c(ParticipantIdentifier, 
      LogId, 
      StartDate, 
      EndDate, 
      SleepStartTime, 
      SleepEndTime, 
      MidSleep, 
      Efficiency, 
      NumAwakenings, 
      remOnsetLatency, 
      remFragmentationIndex)
  ) %>% 
  mutate(Date = lubridate::as_date(StartDate))
