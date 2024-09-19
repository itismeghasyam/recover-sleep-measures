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
  )

fitbit_sleeplogdetails <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogdetails"))
  )

sleeplogdetails_vars <- 
  selected_vars %>% 
  filter(str_detect(Export, "sleeplogdetails$")) %>% 
  pull(Variable)

sleeplogdetails_df <- 
  fitbit_sleeplogdetails %>% 
  select(all_of(sleeplogdetails_vars)) %>% 
  collect() %>% 
  distinct() %>% 
  left_join(y = (sleeplogs_df %>% select(ParticipantIdentifier, LogId, Date, IsMainSleep)), 
            by = join_by("LogId", "ParticipantIdentifier")) %>% 
  group_by(ParticipantIdentifier, LogId, id) %>% 
  arrange(StartDate, .by_group = TRUE) %>% 
  ungroup() %>% 
  mutate(SleepStatus = ifelse(Value %in% c("wake", "awake"), 0, 1))

sleeplogdetails_df_unique <- sleeplogdetails_df[!duplicated(sleeplogdetails_df),]

tictoc::tic()
ex_weekly <- 
  lapply(unique(sleeplogdetails_df_unique$ParticipantIdentifier)[1], function(pid) {
    df <- 
      sleeplogdetails_df_unique %>% 
      filter(ParticipantIdentifier==pid) %>%
      select(ParticipantIdentifier, LogId, StartDate, EndDate, Value, Type) %>% 
      mutate(StartDate = as_datetime(StartDate), EndDate = as_datetime(EndDate)) %>% 
      filter(!is.na(StartDate), !is.na(EndDate)) %>% 
      mutate(SleepStatus = ifelse(Value %in% c("wake", "awake"), 0, 1)) %>% 
      rowwise() %>%
      reframe(
        ParticipantIdentifier = ParticipantIdentifier,
        LogId = LogId,
        DateTime = seq(StartDate, EndDate, by = "30 sec"),
        Value = Value,
        Type = Type,
        SleepStatus = SleepStatus
      ) %>%
      ungroup() %>% 
      group_by(ParticipantIdentifier, LogId) %>%
      arrange(DateTime, .by_group = TRUE) %>% 
      ungroup()
    
    lapply(unique(df$LogId), function(lid) {
      df %>% 
        filter(LogId==lid) %>% 
        rowwise() %>% 
        reframe(
          ParticipantIdentifier = ParticipantIdentifier,
          LogId = LogId,
          DateTime = seq(floor_date(min(DateTime), "day"), ceiling_date(max(DateTime), "day"), by = "30 sec"),
          Value = Value,
          Type = Type,
          SleepStatus = SleepStatus
        )
      }) %>% 
      bind_rows()
  }) %>% 
  bind_rows()
tictoc::toc()

tictoc::tic()
ex_sliding3weeks <- 
  lapply(unique(sleeplogdetails_df_unique$ParticipantIdentifier)[0:5], function(pid) {
    sleeplogdetails_df_unique %>% 
      filter(ParticipantIdentifier==pid) %>%
      select(ParticipantIdentifier, LogId, StartDate, EndDate, Value, Type) %>% 
      mutate(StartDate = as_datetime(StartDate), EndDate = as_datetime(EndDate)) %>% 
      filter(!is.na(StartDate), !is.na(EndDate)) %>% 
      mutate(SleepStatus = ifelse(Value %in% c("wake", "awake"), 0, 1)) %>% 
      rowwise() %>%
      reframe(
        ParticipantIdentifier = ParticipantIdentifier,
        LogId = LogId,
        DateTime = seq(StartDate, EndDate, by = "30 sec"),
        Value = Value,
        Type = Type,
        SleepStatus = SleepStatus
      ) %>%
      ungroup() %>% 
      group_by(ParticipantIdentifier, LogId) %>%
      arrange(DateTime, .by_group = TRUE) %>% 
      ungroup()
  }) %>% 
  bind_rows()
tictoc::toc()

tictoc::tic()
ex_alltime <- 
  lapply(unique(sleeplogdetails_df_unique$ParticipantIdentifier)[0:5], function(pid) {
    sleeplogdetails_df_unique %>% 
      filter(ParticipantIdentifier==pid) %>%
      select(ParticipantIdentifier, LogId, StartDate, EndDate, Value, Type) %>% 
      mutate(StartDate = as_datetime(StartDate), EndDate = as_datetime(EndDate)) %>% 
      filter(!is.na(StartDate), !is.na(EndDate)) %>% 
      mutate(SleepStatus = ifelse(Value %in% c("wake", "awake"), 0, 1)) %>% 
      rowwise() %>%
      reframe(
        ParticipantIdentifier = ParticipantIdentifier,
        LogId = LogId,
        DateTime = seq(StartDate, EndDate, by = "30 sec"),
        Value = Value,
        Type = Type,
        SleepStatus = SleepStatus
      ) %>%
      ungroup() %>% 
      group_by(ParticipantIdentifier, LogId) %>%
      arrange(DateTime, .by_group = TRUE) %>% 
      ungroup()
  }) %>% 
  bind_rows()
tictoc::toc()

tictoc::tic()
ex_alltime_post_infection <- 
  lapply(unique(sleeplogdetails_df_unique$ParticipantIdentifier)[0:5], function(pid) {
    sleeplogdetails_df_unique %>% 
      filter(ParticipantIdentifier==pid) %>%
      select(ParticipantIdentifier, LogId, StartDate, EndDate, Value, Type) %>% 
      mutate(StartDate = as_datetime(StartDate), EndDate = as_datetime(EndDate)) %>% 
      filter(!is.na(StartDate), !is.na(EndDate)) %>% 
      mutate(SleepStatus = ifelse(Value %in% c("wake", "awake"), 0, 1)) %>% 
      rowwise() %>%
      reframe(
        ParticipantIdentifier = ParticipantIdentifier,
        LogId = LogId,
        DateTime = seq(StartDate, EndDate, by = "30 sec"),
        Value = Value,
        Type = Type,
        SleepStatus = SleepStatus
      ) %>%
      ungroup() %>% 
      group_by(ParticipantIdentifier, LogId) %>%
      arrange(DateTime, .by_group = TRUE) %>% 
      ungroup()
  }) %>% 
  bind_rows()
tictoc::toc()

# Assuming sleeplogs_df has a column 'SleepStatus' (1 = sleep, 0 = wake)
# Group data by ParticipantIdentifier and calculate SRI
sri_results <- 
  sleeplogdetails_df_unique %>%
  arrange(ParticipantIdentifier, Date, SleepStartTime) %>%  # Ensure the data is sorted correctly
  group_by(ParticipantIdentifier) %>%
  summarise(
    SRI = sri(SleepStatus, epochs_per_day = 2880)
  )

# Weekly statistics
weekly_stats <- 
  list(
    weekly = NULL,
    sliding3weeks = NULL
  )

# All-time statistics
alltime_stats

