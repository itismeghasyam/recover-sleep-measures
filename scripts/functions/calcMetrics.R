#' Calculate metrics for Participant-grouped data
#'
#' @param data A data frame containing a grouped index (such as 
#' ParticipantIdentifier) and variables to calculate metrics or 
#' summary statistics for
#' @param vars Variables in `data` to calculate metrics or summary statistics for
#' @param metrics Metrics or summary statistics to calculate
#' @param primaryDateCol (Optional) The main column in `data` containing a date for each record; defaults to "Date"
#' @param infectionDateCol (OPTIONAL) A column in `data` containing an infection date for each group
#' @param monthsPostInfection If `infectionDateCol` is supplied then filter 
#' `data` to include only the records where `primaryDateCol` >= `infectionDateCol` + `monthsPostInfection`
#'
#' @return An ungrouped data frame
#' @export
#'
#' @examples
#' \dontrun{
#' ## Not run:
#' 
#' # Weekly statistics
#' weekly_stats <-
  # df %>%
  # group_by(ParticipantIdentifier, WeekStart = floor_date(Date, "week")) %>%
  # calcMetrics(vars = c("PercentDeep", "PercentLight", "PercentRem"),
  #             metrics = c("mean", "median", "SD"))
#' 
#' # All-time statistics
#' alltime_stats <-
#'   df %>%
#'   group_by(ParticipantIdentifier) %>%
#'   calcMetrics(vars = "MidSleep",
#'               metrics = c("circularSD", "count"),
#'               primaryDateCol = "Date",
#'               infectionDateCol = "InfectionFirstReportedDate",
#'               monthsPostInfection = 3)
#' 
#' End(Not run)
#' }
calcMetrics <- function(data, 
                        vars, 
                        metrics,
                        primaryDateCol = "Date",
                        infectionDateCol = NULL,
                        monthsPostInfection = NULL) {
  
  accepted_metrics <- 
    c("mean", "median", "variance",
      "percentile5", "percentile95", "count",
      "percent", "circularSD", "SD")
  
  invalid_metrics <- metrics[!metrics %in% accepted_metrics]
  
  if (length(invalid_metrics) > 0) {
    stop("Invalid 'metrics' argument(s): ", paste(invalid_metrics, collapse = ", "), 
         ". Accepted values are: ", paste(accepted_metrics, collapse = ", "))
  }
  
  if (!is.null(infectionDateCol) & !is.null(monthsPostInfection)) {
    data <- 
      data %>%
      mutate(
        Date = as.Date(!! sym(primaryDateCol)),
        InfectionDate = as.Date(!! sym(infectionDateCol))
      ) %>%
      filter(Date >= InfectionDate + months(monthsPostInfection))

  } else if (is.null(infectionDateCol) != is.null(monthsPostInfection)) {
    stop("Both 'infectionDateCol' and 'monthsPostInfection' must be provided together.")
  }

  summary_functions <-
    list(
      mean = ~mean(.x, na.rm = TRUE),
      median = ~median(.x, na.rm = TRUE),
      variance = ~var(.x, na.rm = TRUE),
      percentile5 = ~quantile(.x, 0.05, na.rm = TRUE),
      percentile95 = ~quantile(.x, 0.95, na.rm = TRUE),
      count = ~sum(!is.na(.x)),
      percent = ~sum(.x, na.rm = TRUE)/sum(!is.na(.x)),
      circularSD = ~psych::circadian.sd(.x, hours = TRUE, na.rm = TRUE)$sd,
      SD = ~stats::sd(.x, na.rm = TRUE)
    )

  selected_functions <- summary_functions[metrics]

  result <-
    data %>%
    summarise(
      across(.cols = all_of(c(vars)),
             .fns = selected_functions,
             .names = "{.col}_{.fn}"),
      .groups = "drop"
    )

  return(result)

}
