#' SRI (Sleep Regularity Index)
#'
#' @param sleep_vec 
#' @param epochs_per_day 
#'
#' @return
#' @export
#'
#' @examples
sri <- function(sleep_vec, epochs_per_day = 1440) {
  
  valid_epochs <- length(sleep_vec) - epochs_per_day
  
  sri <- 200 * mean(sleep_vec[1:valid_epochs] == sleep_vec[(epochs_per_day + 1):(epochs_per_day + valid_epochs)], na.rm = TRUE) - 100
  
  return(sri)
}
