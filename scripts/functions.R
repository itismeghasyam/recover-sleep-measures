timeInSleepStages <- function(data, variable) {
  
  result <- 
    data %>%
    summarise(
      across(.cols = all_of({{variable}}), 
             .fns = 
               list(
                 Mean = ~mean(.x, na.rm = TRUE),
                 Median = ~median(.x, na.rm = TRUE),
                 Variance = ~var(.x, na.rm = TRUE),
                 Percentile5 = ~quantile(.x, 0.05, na.rm = TRUE),
                 Percentile95 = ~quantile(.x, 0.95, na.rm = TRUE),
                 Count = ~n()
               ),
             .names = "{.col}_{.fn}"),
      .groups = "drop"
    )
  
  return(result)
}

# Weekly statistics
weekly_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>%
  calculate_stats(variable = "PercentDeep") %>% 
  ungroup()

# All-time statistics
alltime_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier) %>%
  calculate_stats(variable = "PercentDeep") %>%
  ungroup()

percentBetween8pmAnd2am <- function(data, variable) {
  
  result <-
    data %>%
    drop_na() %>% 
    summarise(
      across(.cols = all_of({{variable}}),
             .fns = 
               list(
                 PercentOfTime = ~sum(.x, na.rm = TRUE)/n(),
                 Count = ~n()
               ),
             .names = "{.fn}"),
      .groups = "drop"
    )
  
  return(result)
}

# Weekly statistics
weekly_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier, Week = floor_date(Date, "week")) %>%
  calculate_stats(variable = "Between8and2") %>%
  ungroup()

# All-time statistics
alltime_stats <- 
  sleeplogs_df %>%
  group_by(ParticipantIdentifier) %>%
  calculate_stats(variable = "Between8and2") %>%
  ungroup()

sleepSD <- function(data, variable, circular = FALSE, infectionDateVar = NULL, monthsPostInfection = NULL) {
  
  if (!is.null(infectionDateVar) & !is.null(monthsPostInfection)) {
    filtered_data <-
      data %>%
      filter(Date >= !!sym(infectionDateVar) + months(monthsPostInfection))
  } else {
    print("infectionDateVar AND monthsPostInfection both need to be non-NULL")
    filtered_data <- data
  }
  
  if (circular) {
    result <-
      filtered_data %>%
      summarise(
        circular_sd = psych::circadian.sd(!!sym(variable), hours = TRUE, na.rm = TRUE)$sd,
        count = n(),
        .groups = "drop"
      )
  } else {
    result <-
      filtered_data %>%
      summarise(
        sd = stats::sd(!!sym(variable), na.rm = TRUE),
        count = n(),
        .groups = "drop"
      )
  }
  
  return(result)
}

# Weekly statistics
weekly_stats <- 
  list(
    midsleep = 
      merged_data %>%
      group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>%
      calculate_stats(variable = "MidSleep", 
                      circular = TRUE, 
                      infectionDateVar = "InfectionFirstReportedDate", 
                      monthsPostInfection = 3) %>%
      ungroup(),
    duration = 
      merged_data %>%
      group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>%
      calculate_stats(variable = "Duration", 
                      circular = FALSE) %>%
      ungroup()
  )

# All-time statistics
alltime_stats <- 
  list(
    midsleep =
      merged_data %>%
      group_by(ParticipantIdentifier) %>%
      calculate_stats(variable = "MidSleep", 
                      circular = TRUE, 
                      infectionDateVar = "InfectionFirstReportedDate", 
                      monthsPostInfection = 6) %>%
      ungroup(),
    duration =
      merged_data %>%
      group_by(ParticipantIdentifier) %>%
      calculate_stats(variable = "Duration", 
                      circular = FALSE) %>%
      ungroup()
  )
