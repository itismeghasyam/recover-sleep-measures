source("scripts/etl/fetch-data.R")
source("scripts/functions/sri.R")

library(future.apply)

# Set the number of cores for parallel processing to 75% of available cores
num_cores <- floor(parallel::detectCores()*0.75)
plan(multisession, workers = num_cores)

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

# Split dataset_paths into two halves for processing
dataset_paths <- list.dirs(merged_df_path)
dataset_paths <- dataset_paths[dataset_paths != merged_df_path]

half_size <- ceiling(length(dataset_paths) / 2)
dataset_paths_first_half <- dataset_paths[1:half_size]
dataset_paths_second_half <- dataset_paths[(half_size + 1):length(dataset_paths)]

# All-time timescale summarization
# Run the first half of the datasets in parallel
tictoc::tic()
results_first_half <- 
  future_lapply(
    dataset_paths_first_half, 
    calc_sri_parallel) %>% 
  bind_rows()
tictoc::toc()

# Run the second half of the datasets in parallel
tictoc::tic()
results_second_half <- 
  future_lapply(
    dataset_paths_second_half, 
    calc_sri_parallel) %>% 
  bind_rows()
tictoc::toc()

# Combine the results from both halves
final_results <- 
  bind_rows(results_first_half, results_second_half)

# All-time 3 months post-infection results
results_first_half_post_infection_3 <- 
  future_lapply(
    dataset_paths_first_half, 
    calc_sri_parallel, 
    months(3)) %>% 
  bind_rows()

results_second_half_post_infection_3 <- 
  future_lapply(
    dataset_paths_second_half, 
    calc_sri_parallel, 
    months(3)) %>% 
  bind_rows()

final_results_post_infection_3 <- 
  bind_rows(results_first_half_post_infection_3, results_second_half_post_infection_3)

# All-time 6 months post-infection results
results_first_half_post_infection_6 <- 
  future_lapply(
    dataset_paths_first_half, 
    calc_sri_parallel, 
    months(6)) %>% 
  bind_rows()

results_second_half_post_infection_6 <- 
  future_lapply(dataset_paths_second_half, calc_sri_parallel, months(6)) %>% 
  bind_rows()

final_results_post_infection_6 <- 
  bind_rows(results_first_half_post_infection_6, results_second_half_post_infection_6)

# Weekly timescale summarization
tictoc::tic()
weekly_results_first_half <- 
  future_lapply(
    dataset_paths_first_half, 
    calc_sri_parallel_weekly) %>% 
  bind_rows()
tictoc::toc()

tictoc::tic()
weekly_results_second_half <- 
  future_lapply(
    dataset_paths_second_half, 
    calc_sri_parallel_weekly) %>% 
  bind_rows()
tictoc::toc()

weekly_results <- 
  bind_rows(weekly_results_first_half, weekly_results_second_half)

# Weekly sliding window timescale summarization
tictoc::tic()
weekly_sliding_results_first_half <- 
  future_lapply(
    dataset_paths_first_half, 
    calc_sri_parallel_weekly, 
    sliding_window = TRUE,
    window_size = 3, 
    step_size = 1) %>% 
  bind_rows()
tictoc::toc()

tictoc::tic()
weekly_sliding_results_second_half <- 
  future_lapply(
    dataset_paths_second_half, 
    calc_sri_parallel_weekly, 
    sliding_window = TRUE,
    window_size = 3, 
    step_size = 1) %>% 
  bind_rows()
tictoc::toc()

weekly_sliding_results <- 
  bind_rows(weekly_results_first_half, weekly_results_second_half)

# 6 months (26 weeks) sliding window timescale summarization
tictoc::tic()
halfyearly_sliding_results_first_half <- 
  future_lapply(
    dataset_paths_first_half, 
    calc_sri_parallel_weekly, 
    sliding_window = TRUE,
    window_size = 26, 
    step_size = 1) %>% 
  bind_rows()
tictoc::toc()

tictoc::tic()
halfyearly_sliding_results_second_half <- 
  future_lapply(
    dataset_paths_second_half, 
    calc_sri_parallel_weekly, 
    sliding_window = TRUE,
    window_size = 26, 
    step_size = 1) %>% 
  bind_rows()
tictoc::toc()

halfyearly_sliding_results <- 
  bind_rows(halfyearly_results_first_half, halfyearly_results_second_half)


# Weekly timescale results
weekly_stats <- 
  list(
    weekly = weekly_results,
    sliding = weekly_sliding_results,
    halfyearly = halfyearly_sliding_results
  )

# All-time timescale results
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
  x = weekly_stats$halfyearly, 
  file = file.path(outputDataDirSRI, "weekly_sliding_26_weeks_stats_sri.csv")
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
  mutate(executed = thisScriptUrl,
         used = c(parquetDirId, visits_file_path))

write_tsv(manifest, manifest_path)

synclient <- reticulate::import("synapseclient")
syn_temp <- synclient$Synapse()
syn_temp$login()

synutils <- reticulate::import("synapseutils")
synutils$syncToSynapse(syn = syn_temp, manifestFile = manifest_path)
