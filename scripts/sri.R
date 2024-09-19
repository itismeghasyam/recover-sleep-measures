source("scripts/etl/fetch-data.R")

library(future.apply)
num_cores <- parallel::detectCores()*0.75
plan(multisession, workers = num_cores)

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
  distinct()

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

calc_sri <- function(complete_exdf, epochs_per_day = 2880) {
  200 * mean(
    complete_exdf$SleepStatus[1:(nrow(complete_exdf) - epochs_per_day)] == 
      complete_exdf$SleepStatus[(epochs_per_day + 1):nrow(complete_exdf)]
  ) - 100
}

calc_sri_parallel <- function(dataset_path) {
  
  exdf <- 
    arrow::open_dataset(dataset_path) %>% 
    collect() %>% 
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
    exdf %>%
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
  complete_exdf <- full_intervals %>%
    left_join(exdf, by = "DateTime") %>% 
    mutate(SleepStatus = replace_na(SleepStatus, NA)) %>% 
    mutate(SleepStatus = ifelse(is.na(id) & is.na(SleepStatus), 0, SleepStatus))
  
  ex_sri <- calc_sri(complete_exdf, epochs_per_day = 2880)
  
  participant <- basename(dataset_path)
  
  return(data.frame(participant = participant, sri = ex_sri))
}

result <- data.frame(participant = character(), sri = numeric())

dataset_paths <- list.dirs(merged_df_path)
dataset_paths <- dataset_paths[dataset_paths != merged_df_path]

tictoc::tic("SRI calculation")
result <- future_lapply(dataset_paths, calc_sri_parallel) %>% bind_rows()
tictoc::toc()

# Weekly statistics
weekly_stats <- 
  list(
    weekly = NULL,
    sliding3weeks = NULL
  )

# All-time statistics
alltime_stats <-
  list(
    alltime = NULL,
    start3monthspostinfection = NULL,
    start6monthspostinfection = NULL
  )

