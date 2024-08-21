participants <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "enrolledparticipants$"))
  ) %>% 
  select(c("ParticipantIdentifier",
           "EnrollmentDate",
           "CustomFields_InfectionFirstReportedDate")) %>% 
  collect() %>% 
  mutate(InfectionFirstReportedDate = as.Date(CustomFields_InfectionFirstReportedDate)) %>% 
  select(-CustomFields_InfectionFirstReportedDate)

fitbit_sleeplogs <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogs$"))
  )

vars <- 
  c("ParticipantIdentifier", 
    "LogId",
    "IsMainSleep",
    "StartDate", # YYYY-MM-DDTHH:MM:SS format
    "EndDate",
    "Duration")

sleeplogs_df <- 
  fitbit_sleeplogs %>% 
  select(all_of(c(vars))) %>% 
  collect() %>% 
  distinct() %>% 
  mutate(
    Date = lubridate::as_date(StartDate),
    IsMainSleep = as.logical(IsMainSleep),
    Duration = as.numeric(Duration),
    SleepStartTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, StartDate, NA)),
    SleepEndTime = lubridate::as_datetime(ifelse(IsMainSleep==TRUE, EndDate, NA)),
    MidSleep = format((lubridate::as_datetime(SleepStartTime) + ((Duration/1000)/2)), format = "%H:%M:%S"),
    SleepStartTime = ((format(SleepStartTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
    SleepEndTime = ((format(SleepEndTime, format = "%H:%M:%S") %>% lubridate::hms()) / lubridate::hours(24))*24,
    MidSleep = 24*lubridate::hms(MidSleep)/lubridate::hours(24)
  ) %>% 
  filter(IsMainSleep==TRUE)

merged_data <- 
  sleeplogs_df %>% 
  left_join((participants %>% select(ParticipantIdentifier, InfectionFirstReportedDate)), 
            by = "ParticipantIdentifier")

calculate_stats <- function(data, variable, monthsPostInfection=0) {
  filtered_data <- 
    data %>%
    filter(Date >= InfectionDate + months(months_post_infection))
  
  filtered_data %>% 
    # drop_na() %>% 
    summarise(
      sd = stats::sd({{variable}}, na.rm = TRUE),
      count = n()
    )
}

# Weekly statistics
weekly_stats <- 
  list(
    midsleep = 
      merged_data %>%
      group_by(ParticipantIdentifier, Week = floor_date(Date, "week")) %>%
      calculate_stats(MidSleep, monthsPostInfection = 3) %>%
      ungroup(),
    duration = 
      sleeplogs_df %>%
      group_by(ParticipantIdentifier, Week = floor_date(Date, "week")) %>%
      calculate_stats(Duration, monthsPostInfection = 6) %>%
      ungroup()
  )

# All-time statistics
alltime_stats <- 
  list(
    midsleep =
      sleeplogs_df %>%
      group_by(ParticipantIdentifier) %>%
      calculate_stats(MidSleep) %>%
      ungroup(),
    duration =
      sleeplogs_df %>%
      group_by(ParticipantIdentifier) %>%
      calculate_stats(Duration) %>%
      ungroup()
  )
