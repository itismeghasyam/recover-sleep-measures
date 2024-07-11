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
df <- 
  fitbit_sleeplogs %>% 
  select(all_of(c(vars, "LogId"))) %>% 
  collect() %>% 
  distinct() %>% 
  mutate(Duration = as.numeric(Duration),
         Efficiency = as.numeric(Efficiency),
         IsMainSleep = as.logical(IsMainSleep),
         SleepStartTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, StartDate, NA)),
         SleepEndTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, EndDate, NA)),
         MidSleep = format((lubridate::as_datetime(SleepStartTime) + ((Duration/1000)/2)), format = "%H:%M:%S"),
         SleepStartTime = 
           ((format(SleepStartTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
         SleepEndTime = 
           ((format(SleepEndTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
         MidSleep = 24*lubridate::hms(MidSleep)/lubridate::hours(24))

numEpisodes <- 
  df %>% 
  select(ParticipantIdentifier, StartDate, LogId) %>% 
  mutate(StartDate = lubridate::as_date(StartDate),
         episode = ifelse(!is.na(LogId), 1, NA)) %>% 
  count(ParticipantIdentifier, StartDate, wt = episode, sort = TRUE)
