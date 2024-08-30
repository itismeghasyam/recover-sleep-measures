source("scripts/fetch-data.R")

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

# Weekly statistics
weekly_stats <- 
  list(
    midsleep =
      list(
        weekly =
          merged_data %>%
          group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>%
          summarise(
            circular_sd = psych::circadian.sd(MidSleep, hours = TRUE, na.rm = TRUE)$sd,
            count = sum(!is.na(MidSleep)),
            .groups = "drop"
          ) %>% 
          ungroup(),
        sliding3weeks = NULL # TODO: 3 weeks sliding window
      ),
    duration =
      list(
        weekly =
          merged_data %>%
          group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>%
          summarise(
            sd = stats::sd(Duration, na.rm = TRUE),
            count = sum(!is.na(Duration)),
            .groups = "drop"
          ) %>% 
          ungroup(),
        sliding3weeks = NULL # TODO: 3 weeks sliding window
      )
  )

# All-time statistics
alltime_stats <- 
  list(
    midsleep =
      list(
        alltime =
          merged_data %>%
          group_by(ParticipantIdentifier) %>%
          summarise(
            circular_sd = psych::circadian.sd(MidSleep, hours = TRUE, na.rm = TRUE)$sd,
            count = sum(!is.na(MidSleep)),
            .groups = "drop"
          ) %>% 
          ungroup(),
        start3monthsPostInfection =
          merged_data %>%
          group_by(ParticipantIdentifier) %>%
          filter(Date >= (InfectionFirstReportedDate + months(3))) %>% 
          summarise(
            circular_sd = psych::circadian.sd(MidSleep, hours = TRUE, na.rm = TRUE)$sd,
            count = sum(!is.na(MidSleep)),
            .groups = "drop"
          ) %>% 
          ungroup(),
        start6monthspostinfection =
          merged_data %>%
          group_by(ParticipantIdentifier) %>%
          filter(Date >= (InfectionFirstReportedDate + months(6))) %>% 
          summarise(
            circular_sd = psych::circadian.sd(MidSleep, hours = TRUE, na.rm = TRUE)$sd,
            count = sum(!is.na(MidSleep)),
            .groups = "drop"
          ) %>% 
          ungroup()
      ),
    duration =
      list(
        alltime =
          merged_data %>%
          group_by(ParticipantIdentifier) %>%
          summarise(
            sd = stats::sd(Duration, na.rm = TRUE),
            count = sum(!is.na(Duration)),
            .groups = "drop"
          ) %>% 
          ungroup(),
        start3monthspostinfection =
          merged_data %>%
          group_by(ParticipantIdentifier) %>%
          filter(Date >= (InfectionFirstReportedDate + months(3))) %>% 
          summarise(
            sd = stats::sd(Duration, na.rm = TRUE),
            count = sum(!is.na(Duration)),
            .groups = "drop"
          ) %>% 
          ungroup(),
        start6monthspostinfection =
          merged_data %>%
          group_by(ParticipantIdentifier) %>%
          filter(Date >= (InfectionFirstReportedDate + months(6))) %>% 
          summarise(
            sd = stats::sd(Duration, na.rm = TRUE),
            count = sum(!is.na(Duration)),
            .groups = "drop"
          ) %>% 
          ungroup()
      )
  )
