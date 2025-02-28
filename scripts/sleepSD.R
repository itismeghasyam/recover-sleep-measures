source("scripts/etl/fetch-data.R")

# Load and filter infections data from the provided CSV file
visits_file_path <- readline("Enter path to 'visits' csv file: ")

infections <-
  read_csv(visits_file_path) %>% 
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
        sliding3weeks =
          lapply(unique(merged_data$ParticipantIdentifier), function(pid) {
            merged_data %>% 
              filter(ParticipantIdentifier==pid) %>%
              group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>% 
              arrange(Date, .by_group = TRUE) %>% 
              slider::slide_period(
                .i = .$WeekStart,
                .period = "week",
                .f = ~summarise(
                  .x, 
                  sd = psych::circadian.sd(.x$MidSleep, hours = TRUE, na.rm = TRUE)$sd,
                  count = sum(!is.na(.x$MidSleep)),
                  .groups = "drop"),
                .every = 1,
                .before = 2,
                .complete = FALSE) %>% 
              lapply(function(x) {
                x %>% 
                  mutate(period_start = first(WeekStart), period_end = as.Date(ceiling_date(last(WeekStart), unit = "week"))) %>% 
                  select(-WeekStart) %>% 
                  distinct()
              }) %>% 
              bind_rows()
          }) %>% 
          bind_rows() %>% 
          ungroup() %>% 
          mutate(period_dt = as.numeric(period_end - period_start)) %>% 
          filter(period_dt==21),
        sliding26weeks =
          lapply(unique(merged_data$ParticipantIdentifier), function(pid) {
            merged_data %>% 
              filter(ParticipantIdentifier==pid) %>%
              group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>% 
              arrange(Date, .by_group = TRUE) %>% 
              slider::slide_period(
                .i = .$WeekStart,
                .period = "week",
                .f = ~summarise(
                  .x, 
                  sd = psych::circadian.sd(.x$MidSleep, hours = TRUE, na.rm = TRUE)$sd,
                  count = sum(!is.na(.x$MidSleep)),
                  .groups = "drop"),
                .every = 1,
                .before = 25,
                .complete = FALSE) %>% 
              lapply(function(x) {
                x %>% 
                  mutate(period_start = first(WeekStart), period_end = as.Date(ceiling_date(last(WeekStart), unit = "week"))) %>% 
                  select(-WeekStart) %>% 
                  distinct()
              }) %>% 
              bind_rows()
          }) %>% 
          bind_rows() %>% 
          ungroup() %>% 
          mutate(period_dt = as.numeric(period_end - period_start)) %>% 
          filter(period_dt==182)
        
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
        sliding3weeks =
          lapply(unique(merged_data$ParticipantIdentifier), function(pid) {
            merged_data %>% 
              filter(ParticipantIdentifier==pid) %>%
              group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>% 
              arrange(Date, .by_group = TRUE) %>% 
              slider::slide_period(
                .i = .$WeekStart,
                .period = "week",
                .f = ~summarise(
                  .x, 
                  sd = stats::sd(.x$Duration, na.rm = TRUE),
                  count = sum(!is.na(.x$Duration)),
                  .groups = "drop"),
                .every = 1,
                .before = 2,
                .complete = FALSE) %>% 
              lapply(function(x) {
                x %>% 
                  mutate(period_start = first(WeekStart), period_end = as.Date(ceiling_date(last(WeekStart), unit = "week"))) %>% 
                  select(-WeekStart) %>% 
                  distinct()
              }) %>% 
              bind_rows()
          }) %>% 
          bind_rows() %>% 
          ungroup() %>% 
          mutate(period_dt = as.numeric(period_end - period_start)) %>% 
          filter(period_dt==21),
        sliding26weeks =
          lapply(unique(merged_data$ParticipantIdentifier), function(pid) {
            merged_data %>% 
              filter(ParticipantIdentifier==pid) %>%
              group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>% 
              arrange(Date, .by_group = TRUE) %>% 
              slider::slide_period(
                .i = .$WeekStart,
                .period = "week",
                .f = ~summarise(
                  .x, 
                  sd = stats::sd(.x$Duration, na.rm = TRUE),
                  count = sum(!is.na(.x$Duration)),
                  .groups = "drop"),
                .every = 1,
                .before = 25,
                .complete = FALSE) %>% 
              lapply(function(x) {
                x %>% 
                  mutate(period_start = first(WeekStart), period_end = as.Date(ceiling_date(last(WeekStart), unit = "week"))) %>% 
                  select(-WeekStart) %>% 
                  distinct()
              }) %>% 
              bind_rows()
          }) %>% 
          bind_rows() %>% 
          ungroup() %>% 
          mutate(period_dt = as.numeric(period_end - period_start)) %>% 
          filter(period_dt==182)
        
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
  file = file.path(outputDataDirSleepSD, "weekly_stats_midsleep.csv")
)

write_csv(
  x = weekly_stats$midsleep$sliding3weeks, 
  file = file.path(outputDataDirSleepSD, "sliding3weeks_stats_midsleep.csv")
)

write_csv(
  x = weekly_stats$midsleep$sliding26weeks, 
  file = file.path(outputDataDirSleepSD, "sliding26weeks_stats_midsleep.csv")
)


write_csv(
  x = weekly_stats$duration$weekly, 
  file = file.path(outputDataDirSleepSD, "weekly_stats_duration.csv")
)

write_csv(
  x = weekly_stats$duration$sliding3weeks, 
  file = file.path(outputDataDirSleepSD, "sliding3weeks_stats_duration.csv")
)

write_csv(
  x = weekly_stats$duration$sliding26weeks, 
  file = file.path(outputDataDirSleepSD, "sliding26weeks_stats_duration.csv")
)

write_csv(
  x = alltime_stats$midsleep$alltime, 
  file = file.path(outputDataDirSleepSD, "alltime_stats_midsleep.csv")
)

write_csv(
  x = alltime_stats$midsleep$start3monthsPostInfection, 
  file = file.path(outputDataDirSleepSD, "alltime_stats_midsleep_3mo_post_infection.csv")
)

write_csv(
  x = alltime_stats$midsleep$start6monthspostinfection, 
  file = file.path(outputDataDirSleepSD, "alltime_stats_midsleep_6mo_post_infection.csv")
)

write_csv(
  x = alltime_stats$duration$alltime, 
  file = file.path(outputDataDirSleepSD, "alltime_stats_duration.csv")
)

write_csv(
  x = alltime_stats$duration$start3monthspostinfection, 
  file = file.path(outputDataDirSleepSD, "alltime_stats_duration_3mo_post_infection.csv")
)

write_csv(
  x = alltime_stats$duration$start6monthspostinfection, 
  file = file.path(outputDataDirSleepSD, "alltime_stats_duration_6mo_post_infection.csv")
)

manifest_path <- file.path(outputDataDirSleepSD, "output-data-manifest.tsv")

synapserutils::generate_sync_manifest(
  directory_path = outputDataDirSleepSD,
  parent_id = sleepSDSynDirId,
  manifest_path = manifest_path
)

manifest <- read_tsv(manifest_path)

thisScriptUrl <- "https://github.com/Sage-Bionetworks/recover-sleep-measures/blob/main/scripts/sleepSD.R"

manifest <-
  manifest %>%
  mutate(executed = thisScriptUrl,
         used = c(parquetDirId, visits_file_path))

write_tsv(manifest, manifest_path)

synclient <- reticulate::import("synapseclient")
syn_temp <- synclient$Synapse()
syn_temp$login()

synutils <- reticulate::import("synapseutils")
synutils$syncToSynapse(syn = syn_temp, manifestFile = manifest_path)
