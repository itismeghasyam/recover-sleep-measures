source("scripts/etl/fetch-data.R")

infections <- 
  read_csv(readline("Enter path to 'visits' csv file: ")) %>%
  filter(infect_yn_curr==1) %>%
  group_by(record_id) %>%
  summarise(
    first_infection_date = min(c(as_date(index_dt_curr), as_date(newinf_dt)), na.rm = TRUE),
    last_infection_date = max(c(as_date(index_dt_curr), as_date(newinf_dt)), na.rm = TRUE)
  ) %>%
  rename(ParticipantIdentifier = record_id)

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
  left_join(y = infections, 
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
          filter(Date >= (first_infection_date + months(3))) %>% 
          summarise(
            circular_sd = psych::circadian.sd(MidSleep, hours = TRUE, na.rm = TRUE)$sd,
            count = sum(!is.na(MidSleep)),
            .groups = "drop"
          ) %>% 
          ungroup(),
        start6monthspostinfection =
          merged_data %>%
          group_by(ParticipantIdentifier) %>%
          filter(Date >= (first_infection_date + months(6))) %>% 
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
          filter(Date >= (first_infection_date + months(3))) %>% 
          summarise(
            sd = stats::sd(Duration, na.rm = TRUE),
            count = sum(!is.na(Duration)),
            .groups = "drop"
          ) %>% 
          ungroup(),
        start6monthspostinfection =
          merged_data %>%
          group_by(ParticipantIdentifier) %>%
          filter(Date >= (first_infection_date + months(6))) %>% 
          summarise(
            sd = stats::sd(Duration, na.rm = TRUE),
            count = sum(!is.na(Duration)),
            .groups = "drop"
          ) %>% 
          ungroup()
      )
  )

write_csv(
  x = weekly_stats$midsleep$weekly, 
  file = file.path(outputDataDir, "weekly_stats_midsleep.csv")
)

write_csv(
  x = weekly_stats$duration$weekly, 
  file = file.path(outputDataDir, "weekly_stats_duration.csv")
)

write_csv(
  x = alltime_stats$midsleep$alltime, 
  file = file.path(outputDataDir, "alltime_stats_midsleep.csv")
)

write_csv(
  x = alltime_stats$midsleep$start3monthsPostInfection, 
  file = file.path(outputDataDir, "alltime_stats_midsleep_3mo_post_infection.csv")
)

write_csv(
  x = alltime_stats$midsleep$start6monthspostinfection, 
  file = file.path(outputDataDir, "alltime_stats_midsleep_6mo_post_infection.csv")
)

write_csv(
  x = alltime_stats$duration$alltime, 
  file = file.path(outputDataDir, "alltime_stats_duration.csv")
)

write_csv(
  x = alltime_stats$duration$start3monthspostinfection, 
  file = file.path(outputDataDir, "alltime_stats_duration_3mo_post_infection.csv")
)

write_csv(
  x = alltime_stats$duration$start6monthspostinfection, 
  file = file.path(outputDataDir, "alltime_stats_duration_6mo_post_infection.csv")
)

synapserutils::generate_sync_manifest(
  directory_path = outputDataDir,
  parent_id = derivedMeasuresSynDirId,
  manifest_path = "output-data-manifest.tsv"
)

manifest <- read_tsv("output-data-manifest.tsv")

thisScriptUrl <- "https://github.com/Sage-Bionetworks/recover-sleep-measures/blob/main/scripts/sleepSD.R"

manifest <-
  manifest %>%
  mutate(executed = thisScriptUrl)

write_tsv(manifest, "output-data-manifest.tsv")

synclient <- reticulate::import("synapseclient")
syn_temp <- synclient$Synapse()
syn_temp$login()

synutils <- reticulate::import("synapseutils")
synutils$syncToSynapse(syn = syn_temp, manifestFile = "output-data-manifest.tsv")
