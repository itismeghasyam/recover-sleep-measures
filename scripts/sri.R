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

tictoc::tic("SRI calculation")
result <- data.frame(participant = character(), sri = numeric())

dataset_path <- list.dirs(merged_df_path)
dataset_path <- dataset_path[dataset_path != merged_df_path][1]

calc_sri <- function(complete_exdf, epochs_per_day = 2880) {
  200 * mean(
    complete_exdf$SleepStatus[1:(nrow(complete_exdf) - epochs_per_day)] == 
      complete_exdf$SleepStatus[(epochs_per_day + 1):nrow(complete_exdf)]
  ) - 100
}

exdf <- 
  arrow::open_dataset(dataset_path) %>% 
  collect() %>% 
  arrange(StartDate) %>% 
  # group_by(LogId, id) %>% 
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

# Step 1: Get unique dates
unique_days <- exdf %>%
  mutate(Date = as.Date(DateTime)) %>% 
  distinct(Date)

# Step 2: Generate a complete sequence of 30-second intervals for each day
full_intervals <- unique_days %>%
  rowwise() %>%
  mutate(DateTime = list(seq.POSIXt(as.POSIXct(paste0(Date, " 00:00:00")), 
                                    as.POSIXct(paste0(Date, " 23:59:30")), 
                                    by = "30 sec"))) %>%
  unnest(cols = c(DateTime))

# Step 3: Merge the full sequence with the original data
complete_exdf <- full_intervals %>%
  left_join(exdf, by = "DateTime")

# Step 4: Fill missing SleepStatus values with NA
complete_exdf <- complete_exdf %>%
  mutate(SleepStatus = replace_na(SleepStatus, NA)) %>% 
  mutate(SleepStatus = ifelse(is.na(id) & is.na(SleepStatus), 0, SleepStatus))

epochs_per_day <- 2880

ex_sri <- calc_sri(complete_exdf, epochs_per_day)

participant <- basename(dataset_path)

result <- 
  bind_rows(result, 
            data.frame(participant = participant, sri = ex_sri))
tictoc::toc()

# Assuming sleeplogs_df has a column 'SleepStatus' (1 = sleep, 0 = wake)
# Group data by ParticipantIdentifier and calculate SRI
sri_results <- 
  merged_df_unique %>%
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

