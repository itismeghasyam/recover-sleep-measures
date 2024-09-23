# Functions to calculate Sleep Regularity Index (SRI)
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
  unscaled_sri <- (participant_sri + 100) / 200
  
  participant <- basename(dataset_path)
  
  return(data.frame("ParticipantIdentifier" = participant, unscaled_sri = unscaled_sri, sri = participant_sri))
}

# Function to calculate weekly SRI for each participant
calc_weekly_sri <- function(df, epochs_per_day = 2880) {
  
  # Group data by week
  df <- 
    df %>%
    mutate(Week = as.Date(floor_date(DateTime, unit = "week")))
  
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
