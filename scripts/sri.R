source("scripts/etl/fetch-data.R")

library(future.apply)

# Set the number of cores for parallel processing to 75% of available cores
num_cores <- parallel::detectCores()*0.75
plan(multisession, workers = num_cores)

# Load and filter infections data from the provided CSV file
infections <-
  read_csv(readline("Enter path to 'visits' csv file: ")) %>% 
  filter(infect_yn_curr==1) %>% 
  group_by(record_id) %>%
  summarise(
    first_infection_date = min(c(as_date(index_dt_curr), as_date(newinf_dt)), na.rm = TRUE),
    last_infection_date = max(c(as_date(index_dt_curr), as_date(newinf_dt)), na.rm = TRUE)
  ) %>%
  rename(ParticipantIdentifier = record_id)

# Load fitbit sleeplogs dataset and extract relevant variables
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

# Load fitbit sleeplogdetails dataset and extract relevant variables
fitbit_sleeplogdetails <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "sleeplogdetails"))
  )

sleeplogdetails_vars <- 
  selected_vars %>% 
  filter(str_detect(Export, "sleeplogdetails$")) %>% 
  pull(Variable)

# Preprocess the sleeplogdetails data and join with sleeplogs identifier data
pre_filtered_df <- 
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

# Identify and remove participants with fewer than 2 distinct days of data
# since SRI needs at least 2 days of data
participants_to_remove <- 
  pre_filtered_df %>% 
  group_by(ParticipantIdentifier) %>% 
  summarise(n_days = n_distinct(Date)) %>% 
  filter(n_days<2) %>% 
  pull(ParticipantIdentifier)

filtered_df <- 
  pre_filtered_df %>% 
  filter(!(ParticipantIdentifier %in% participants_to_remove))

# rm(pre_filtered_df)

# Merge filtered data with infections data for use in calculating SRI on a
# timescale involving filtering to include only data N months post-infection
merged_df <- 
  filtered_df %>% 
  left_join(y = infections, by = "ParticipantIdentifier")

# rm(filtered_df)

# Save the data as parquet files partitioned by participant
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

# Function to calculate Sleep Regularity Index (SRI)
# Based on implementation in github.com/mengelhard/sri
calc_sri <- function(df, epochs_per_day = 2880) {
  200 * mean(
    df$SleepStatus[1:(nrow(df) - epochs_per_day)] == 
      df$SleepStatus[(epochs_per_day + 1):nrow(df)]
  ) - 100
}

# Parallel SRI calculation for a given dataset (optionally filter post-infection)
calc_sri_parallel <- function(dataset_path, post_infection = FALSE) {
  
  # Filter data post-infection if specified
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
  
  # Generate 30-second intervals for each sleep event
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
  complete_df <- 
    full_intervals %>%
    left_join(participant_df, by = "DateTime") %>% 
    mutate(SleepStatus = replace_na(SleepStatus, NA)) %>% 
    mutate(SleepStatus = ifelse(is.na(id) & is.na(SleepStatus), 0, SleepStatus))
  
  # Calculate SRI
  participant_sri <- calc_sri(complete_df, epochs_per_day = 2880)
  
  # Unscale SRI based on implementation in github.com/mengelhard/sri
  unscaled_sri <- (sri + 100) / 200
  
  participant <- basename(dataset_path)
  
  return(data.frame("ParticipantIdentifier" = participant, sri = participant_sri, unscaled_sri = unscaled_sri))
}

# Split dataset_paths into two halves for processing
dataset_paths <- list.dirs(merged_df_path)
dataset_paths <- dataset_paths[dataset_paths != merged_df_path]

half_size <- ceiling(length(dataset_paths) / 2)
dataset_paths_first_half <- dataset_paths[1:half_size]
dataset_paths_second_half <- dataset_paths[(half_size + 1):length(dataset_paths)]

# Run the first half of the datasets in parallel
tictoc::tic("SRI calculation for first half")
results_first_half <- future_lapply(dataset_paths_first_half, calc_sri_parallel) %>% bind_rows()
tictoc::toc()

# Run the second half of the datasets in parallel
tictoc::tic("SRI calculation for second half")
results_second_half <- future_lapply(dataset_paths_second_half, calc_sri_parallel) %>% bind_rows()
tictoc::toc()

# Combine the results from both halves
final_results <- bind_rows(results_first_half, results_second_half)

# All-time 3 months post-infection results
results_first_half_post_infection_3 <- 
  future_lapply(dataset_paths_first_half, calc_sri_parallel, months(3)) %>% 
  bind_rows()

results_second_half_post_infection_3 <- 
  future_lapply(dataset_paths_second_half, calc_sri_parallel, months(3)) %>% 
  bind_rows()

final_results_post_infection_3 <- 
  bind_rows(results_first_half_post_infection_3, results_second_half_post_infection_3)

# All-time 6 months post-infection results
results_first_half_post_infection_6 <- 
  future_lapply(dataset_paths_first_half, calc_sri_parallel, months(6)) %>% 
  bind_rows()

results_second_half_post_infection_6 <- 
  future_lapply(dataset_paths_second_half, calc_sri_parallel, months(6)) %>% 
  bind_rows()

final_results_post_infection_6 <- 
  bind_rows(results_first_half_post_infection_6, results_second_half_post_infection_6)


# Function to calculate weekly SRI for each participant
calc_weekly_sri <- function(df, epochs_per_day = 2880) {
  
  # Group data by week
  df <- 
    df %>%
    mutate(Week = floor_date(DateTime, unit = "week"))
  
  # Calculate SRI for each week
  weekly_sri <- 
    df %>%
    group_by(Week) %>%
    summarise(
      unscaled_sri = mean(SleepStatus[1:(n() - epochs_per_day)] == SleepStatus[(epochs_per_day + 1):n()]),
      sri = 200 * unscaled_sri - 100,
      .groups = "drop"
    )
  
  return(weekly_sri)
}

# Function to calculate sliding window weekly SRI for each participant
calc_weekly_sliding_sri <- function(df, epochs_per_day = 2880, window_size = 3, step_size = 1) {
  
  sliding_sri <- 
    df %>% 
    arrange(DateTime) %>% 
    slider::slide_period(
      .i = .$DateTime,
      .period = "week",
      .f = ~summarise(
        .x, 
        period_start = as.Date(first(floor_date(DateTime, "week"))),
        period_end = as.Date(ceiling_date(last(floor_date(DateTime, "week")), "week")),
        unscaled_sri = mean(SleepStatus[1:(n()-epochs_per_day)]==SleepStatus[(epochs_per_day+1):n()]),
        sri = 200 * unscaled_sri - 100,
        .groups = "drop"),
      .every = step_size,
      .before = window_size - 1,
      .complete = FALSE) %>% 
    bind_rows() %>% 
    ungroup() %>% 
    mutate(period_dt = as.numeric(period_end-period_start)) %>% 
    filter(period_dt==21) %>% 
    select(-period_dt)
  
  return(sliding_sri)
}

# Adjust calc_sri_parallel to calculate weekly SRI
calc_sri_parallel_weekly <- function(dataset_path, sliding_window = FALSE, window_size = 3, step_size = 1) {
  
  # Generate 30-second intervals for each sleep event
  participant_df <- 
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
  
  # Get unique dates and generate full 30-second interval sequence for each day
  unique_days <- 
    participant_df %>%
    mutate(Date = as.Date(DateTime)) %>% 
    distinct(Date)
  
  full_intervals <- 
    unique_days %>%
    rowwise() %>%
    mutate(DateTime = list(seq.POSIXt(as.POSIXct(paste0(Date, " 00:00:00")), 
                                      as.POSIXct(paste0(Date, " 23:59:30")), 
                                      by = "30 sec"))) %>%
    unnest(cols = c(DateTime))
  
  # Merge with original data and fill missing SleepStatus
  complete_df <- 
    full_intervals %>%
    left_join(participant_df, by = "DateTime") %>% 
    mutate(SleepStatus = replace_na(SleepStatus, NA)) %>% 
    mutate(SleepStatus = ifelse(is.na(id) & is.na(SleepStatus), 0, SleepStatus))
  
  # Calculate SRI for each week based on implementation in github.com/mengelhard/sri
  if (sliding_window) {
    result <- 
      calc_weekly_sliding_sri(
        complete_df, 
        epochs_per_day = 2880, 
        window_size = window_size, 
        step_size = step_size
      )
  } else {
    result <- 
      calc_weekly_sri(complete_df, epochs_per_day = 2880)
  }
  
  participant <- basename(dataset_path)
  
  return(bind_cols("ParticipantIdentifier" = participant, result))
}

# Run the first half of the datasets in parallel for weekly SRI
tictoc::tic("Weekly SRI calculation for first half")
weekly_results_first_half <- 
  future_lapply(
    dataset_paths_first_half, 
    calc_sri_parallel_weekly) %>% 
  bind_rows()
tictoc::toc()

# Run the second half of the datasets in parallel for weekly SRI
tictoc::tic("Weekly SRI calculation for second half")
weekly_results_second_half <- 
  future_lapply(
    dataset_paths_second_half, 
    calc_sri_parallel_weekly) %>% 
  bind_rows()
tictoc::toc()

# Combine the sliding window weekly results from both halves
weekly_results <- bind_rows(weekly_results_first_half, weekly_results_second_half)

# Run the first half of the datasets in parallel for sliding window weekly SRI
tictoc::tic("Sliding window weekly SRI calculation for first half")
weekly_sliding_results_first_half <- 
  future_lapply(
    dataset_paths_first_half, 
    calc_sri_parallel_weekly, 
    sliding_window = TRUE,
    window_size = 3, 
    step_size = 1) %>% 
  bind_rows()
tictoc::toc()

# Run the second half of the datasets in parallel for sliding window weekly SRI
tictoc::tic("Sliding window weekly SRI calculation for second half")
weekly_sliding_results_second_half <- 
  future_lapply(
    dataset_paths_second_half, 
    calc_sri_parallel_weekly, 
    sliding_window = TRUE,
    window_size = 3, 
    step_size = 1) %>% 
  bind_rows()
tictoc::toc()

# Combine the sliding window weekly results from both halves
weekly_sliding_results <- bind_rows(weekly_results_first_half, weekly_results_second_half)


# Weekly statistics
weekly_stats <- 
  list(
    weekly = weekly_results,
    sliding = weekly_sliding_results
  )

# All-time statistics
alltime_stats <-
  list(
    alltime = final_results,
    post_infection_3_months = final_results_post_infection_3,
    post_infection_6_months = final_results_post_infection_6
  )

# Write output to local dir
write_csv(
  x = weekly_stats$weekly, 
  file = file.path(outputDataDirSRI, "weekly_stats_sri.csv")
)

write_csv(
  x = weekly_stats$sliding, 
  file = file.path(outputDataDirSRI, "weekly_sliding_3_weeks_stats_sri.csv")
)

write_csv(
  x = alltime_stats$alltime, 
  file = file.path(outputDataDirSRI, "alltime_stats_sri.csv")
)

write_csv(
  x = alltime_stats$post_infection_3_months, 
  file = file.path(outputDataDirSRI, "alltime_3_months_post_infection_stats_sri.csv")
)

write_csv(
  x = alltime_stats$post_infection_6_months, 
  file = file.path(outputDataDirSRI, "alltime_6_months_post_infection_stats_sri.csv")
)

# Store in Synapse
manifest_path <- file.path(outputDataDirSRI, "output-data-manifest.tsv")

synapserutils::generate_sync_manifest(
  directory_path = outputDataDirSleepSD,
  parent_id = sriSynDirId,
  manifest_path = manifest_path
)

manifest <- read_tsv(manifest_path)

thisScriptUrl <- "https://github.com/Sage-Bionetworks/recover-sleep-measures/blob/main/scripts/sri.R"

manifest <-
  manifest %>%
  mutate(executed = thisScriptUrl)

write_tsv(manifest, manifest_path)

synclient <- reticulate::import("synapseclient")
syn_temp <- synclient$Synapse()
syn_temp$login()

synutils <- reticulate::import("synapseutils")
synutils$syncToSynapse(syn = syn_temp, manifestFile = manifest_path)

