source("scripts/etl/fetch-data.R")

library(future.apply)
num_cores <- parallel::detectCores()*0.75
plan(multisession, workers = num_cores)

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
    "EndDate" # YYYY-MM-DDTHH:MM:SS format,
  )

# Load desired subset of the data in memory and do some feature engineering
sleeplogs_df <- 
  fitbit_sleeplogs %>% 
  select(all_of(c(vars))) %>% 
  collect() %>% 
  distinct() %>% 
  mutate(
    Date = lubridate::as_date(StartDate), # YYYY-MM-DD format
    IsMainSleep = as.logical(IsMainSleep)
  )

fitbit_sleeplogdetails <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogdetails"))
  )

sleeplogdetails_vars <- 
  selected_vars %>% 
  filter(str_detect(Export, "sleeplogdetails$")) %>% 
  pull(Variable)

merged_df <- 
  fitbit_sleeplogdetails %>% 
  select(all_of(sleeplogdetails_vars)) %>% 
  filter(Type=="SleepLevel") %>% 
  select(-c("Type")) %>% 
  collect() %>% 
  distinct() %>% 
  left_join(y = (sleeplogs_df %>% select(ParticipantIdentifier, LogId, Date, IsMainSleep)), 
            by = join_by("ParticipantIdentifier", "LogId")) %>% 
  group_by(ParticipantIdentifier, LogId, id) %>% 
  arrange(StartDate, .by_group = TRUE) %>% 
  ungroup() %>% 
  mutate(SleepStatus = ifelse(Value %in% c("wake", "awake"), 0, 1)) %>% 
  select(-Value) %>% 
  distinct() %>% 
  left_join(y = infections, by = "ParticipantIdentifier")

# rm(sleeplogs_df)

merged_df_path <- "./temp-output-data/datasets/merged_df"
unlink(merged_df_path, recursive = T, force = T)
dir.create(path = merged_df_path, recursive = T)

tictoc::tic("Writing datasets")
arrow::write_dataset(dataset = merged_df, 
                     path = merged_df_path, 
                     format = "parquet", 
                     partitioning = "ParticipantIdentifier", 
                     hive_style = FALSE)
tictoc::toc()

# rm(merged_df)

calc_sri <- function(df, epochs_per_day = 2880) {
  200 * mean(
    df$SleepStatus[1:(nrow(df) - epochs_per_day)] == 
      df$SleepStatus[(epochs_per_day + 1):nrow(df)]
  ) - 100
}

calc_sri_parallel <- function(dataset_path, post_infection = FALSE) {
  
  if (post_infection) {
    pre_df <- 
      arrow::open_dataset(dataset_path) %>% 
      collect() %>% 
      filter(Date >= (first_infection_date + post_infection))
  } else {
    pre_df <-
      arrow::open_dataset(dataset_path) %>% 
      collect()
  }
  
  participant_df <- 
    pre_df %>% 
    arrange(StartDate) %>% 
    rowwise() %>% 
    reframe(
      LogId = LogId,
      id = id,
      DateTime = seq(min(as_datetime(StartDate)), max(as_datetime(EndDate)), by = "30 sec"),
      SleepStatus = SleepStatus
    ) %>% 
    group_by(DateTime) %>%
    slice_tail(n = 1) %>%
    ungroup()
  
  # Get unique dates
  unique_days <- 
    participant_df %>%
    mutate(Date = as.Date(DateTime)) %>% 
    distinct(Date)
  
  # Generate a complete sequence of 30-second intervals for each day
  full_intervals <- 
    unique_days %>%
    rowwise() %>%
    mutate(DateTime = list(seq.POSIXt(as.POSIXct(paste0(Date, " 00:00:00")), 
                                      as.POSIXct(paste0(Date, " 23:59:30")), 
                                      by = "30 sec"))) %>%
    unnest(cols = c(DateTime))
  
  # Merge the full sequence with the original data and fill missing SleepStatus values with NA
  complete_df <- full_intervals %>%
    left_join(participant_df, by = "DateTime") %>% 
    mutate(SleepStatus = replace_na(SleepStatus, NA)) %>% 
    mutate(SleepStatus = ifelse(is.na(id) & is.na(SleepStatus), 0, SleepStatus))
  
  sri <- calc_sri(complete_df, epochs_per_day = 2880)
  unscaled_sri <- (sri + 100) / 200
  
  participant <- basename(dataset_path)
  
  return(data.frame(participant = participant, sri = participant_sri, unscaled_sri = unscaled_sri))
}

dataset_paths <- list.dirs(merged_df_path)
dataset_paths <- dataset_paths[dataset_paths != merged_df_path]

half_size <- ceiling(length(dataset_paths) / 2)
dataset_paths_first_half <- dataset_paths[1:half_size]
dataset_paths_second_half <- dataset_paths[(half_size + 1):length(dataset_paths)]

# Run the first half of the datasets in parallel
tictoc::tic("SRI calculation for first half")
result_first_half <- future_lapply(dataset_paths_first_half, calc_sri_parallel) %>% bind_rows()
tictoc::toc()

# Run the second half of the datasets in parallel
tictoc::tic("SRI calculation for second half")
result_second_half <- future_lapply(dataset_paths_second_half, calc_sri_parallel) %>% bind_rows()
tictoc::toc()

# Combine the results
final_result <- bind_rows(result_first_half, result_second_half)

# Weekly statistics
weekly_stats <- 
  list(
    weekly = NULL,
    sliding3weeks = NULL
  )

# All-time statistics
alltime_stats <-
  list(
    alltime = final_result,
    start3monthspostinfection = NULL,
    start6monthspostinfection = NULL
  )

