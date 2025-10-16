#######
# Purpose: Study Hf and Lf from fitbit intraday combined, and if it is related to PASC status (Long Covid)
# Author: meghasyam@sagebase.org
#######

#######
# Required Libraries and functions
#######
library(tidyverse)
library(synapser)
library(tictoc)
synapser::synLogin()
ARCHIVE_VERSION <- '2025-02-16'
print(ARCHIVE_VERSION)

# to handle all NA's sum(c(NA, NA)) = 0; but we want it to be NA
## this causes issues with NaN SpO2, Breathing Rate etc
my_sum <- function(x) {
  if (all(is.na(x))) {
    return(as.numeric(NA))
  } else {
    return(sum(x, na.rm = TRUE))
  }
}

my_num_records <- function(x){
  if (all(is.na(x))) {
    return(as.numeric(NA))
  } else {
    return(sum(!is.na(x), na.rm = TRUE))
  }
}

## Weekly summaries from daily summary data

WeeklyHRVSummaries <- function(dat) {
  dat$Date <- lubridate::as_date(dat$Date)
  
  ## for each date we first find the start and end dates of each week 
  ## (each week starts at a Sunday and ends at a Saturday)
  
  ## for each date, find the date of the day of that week (always a Sunday) 
  dat$week_startdate <- lubridate::floor_date(dat$Date, unit = "week", week_start = 7)
  
  ## find the date of the weeks last day (always a Saturday) 
  dat$week_enddate <- dat$week_startdate + lubridate::days(6)
  
  ## get the unique combinations of participant ids and week start dates
  dat$aux <- paste(dat$ParticipantIdentifier, dat$week_startdate, sep = "_")
  ucomb <- unique(dat$aux)
  nrec <- length(ucomb)
  out_mean <- data.frame(matrix(NA, nrec, 7))
  out_sum <- data.frame(matrix(NA, nrec, 3))
  variable_names <- c(
    'mean_rmssd',
    'mean_coverage',
    'mean_hf',
    'mean_lf',
    'mean_ratioF'
  )
  
  # count nDays in the week; nHeartRateIntradayMinuteCount
  variable_names_sum <- c(
    'nrec' # nrecords
  )
  
  names(out_sum) <- c("ParticipantIdentifier", 
                      "Date", 
                      variable_names_sum)
  
  names(out_mean) <- c("ParticipantIdentifier", 
                       "Date", 
                       variable_names)
  
  for (i in seq(nrec)) {
    cat(i, "\n")
    
    sdat <- dat[dat$aux == ucomb[i],]
    
    ## Means
    
    out_mean[i, "ParticipantIdentifier"] <- sdat[1, "ParticipantIdentifier"]
    
    ## we set Date to the week's start date (always a Sunday)
    out_mean[i, "Date"] <- as.character(sdat[1, "week_startdate"][[1]])
    
    out_mean[i, variable_names] <- apply(sdat[, variable_names], 2, mean, na.rm = TRUE)
    
    ### Sums
    
    out_sum[i, "ParticipantIdentifier"] <- sdat[1, "ParticipantIdentifier"]
    
    ## we set Date to the week's start date (always a Sunday)
    out_sum[i, "Date"] <- as.character(sdat[1, "week_startdate"][[1]])
    
    
    out_sum[i, variable_names_sum] <- apply(sdat[, variable_names_sum], 2, my_sum)
    
  }
  
  out_mean <- out_mean %>% 
    dplyr::mutate(summary_metric = 'mean')
  
  out_sum <- out_sum %>% 
    dplyr::mutate(summary_metric = 'total')
  
  out <- out_mean %>% 
    dplyr::full_join(out_sum)
  
  return(out)
}

#######
# Get data from Synapse
#######
################### External Parquet access
sts_token <- synapser::synGetStsStorageToken(entity = 'syn52506069', # sts enabled destination folder
                                             permission = 'read_only',   # request a read only token
                                             output_format = 'json')

s3_external <- arrow::S3FileSystem$create(access_key = sts_token$accessKeyId,
                                          secret_key = sts_token$secretAccessKey,
                                          session_token = sts_token$sessionToken,
                                          region="us-east-1")

base_s3_uri <- paste0(sts_token$bucket, "/", sts_token$baseKey,'/',ARCHIVE_VERSION)
parquet_datasets <- s3_external$GetFileInfo(arrow::FileSelector$create(base_s3_uri, recursive=F))

i <- 0
valid_paths <- character()
for (dataset in parquet_datasets) {
  if (grepl('recover-main-project/main/archive/', dataset$path, perl = T, ignore.case = T)) {
    i <- i+1
    cat(i)
    cat(":", dataset$path, "\n")
    valid_paths <- c(valid_paths, dataset$path)
  }
}

valid_paths_df <- valid_paths %>%
  as.data.frame() %>%
  `colnames<-`('parquet_path') %>%
  dplyr::rowwise() %>%
  dplyr::mutate(datasetType = str_split(parquet_path,'/')[[1]][5]) %>%
  dplyr::ungroup() 

#################### 480 Min wear time from fitbit dailydata
## We are going to use data from those data that have atleast 480Min wear time
fitbit_dailydata <- 
  arrow::open_dataset(
    s3_external$path(stringr::str_subset(valid_paths, "dailydata"))
  )

## days with atleast 480min wear time per participant
weartime_df <- fitbit_dailydata %>% 
  dplyr::select(ParticipantIdentifier, 
                Date,
                HeartRateIntradayMinuteCount) %>% 
  dplyr::collect() %>% 
  dplyr::mutate(HeartRateIntradayMinuteCount = as.numeric(HeartRateIntradayMinuteCount)) %>% 
  dplyr::filter(HeartRateIntradayMinuteCount >= 480) %>%  # HeartRateIntradayMinuteCount > 0, >= 480
  dplyr::mutate(Date = as.Date.character(Date)) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(keyCol = paste0(ParticipantIdentifier, Date, collapse = '-')) %>% 
  dplyr::ungroup()


################### Set 10: dataset_fitbitintradaycombined; Get HRV data
subset_paths_df <- valid_paths_df %>%
  dplyr::filter(datasetType == 'dataset_fitbitintradaycombined')

## get parquet
df_parquet <- arrow::open_dataset(s3_external$path(as.character(subset_paths_df$parquet_path)))

aa <- df_parquet %>% dplyr::slice_head(n = 1000000) %>%  dplyr::filter(Type == 'hrv')  %>% dplyr::collect()

## Get HRV data for which Hf and Lf values exist, i.e Type == 'hrv'
tictoc::tic()
hrv_df <- df_parquet %>% 
  # dplyr::slice_head(n = 1000000000) %>%
  dplyr::filter(Type=='hrv') %>% 
  dplyr::select(ParticipantIdentifier, cohort, DateTime, Type, Rmssd, Coverage, Value, Hf, Lf) %>% 
  dplyr::collect() %>% 
  dplyr::mutate(Date = as.Date.character(DateTime)) 

hrv_df_summaries <- hrv_df %>%
  dplyr::mutate(Lf = as.numeric(Lf)) %>% 
  dplyr::mutate(Hf = as.numeric(Hf)) %>% 
  dplyr::mutate(Rmssd = as.numeric(Rmssd)) %>% 
  dplyr::mutate(Coverage = as.numeric(Coverage)) %>% 
  dplyr::mutate(ratioF = Lf/Hf) %>% 
  dplyr::group_by(ParticipantIdentifier, cohort, Type, Date) %>% 
  dplyr::summarise(max_rmssd = max(Rmssd, na.rm = T),
                   min_rmssd = min(Rmssd, na.rm = T),
                   mean_rmssd = mean(Rmssd, na.rm = T),
                   sd_rmssd = sd(Rmssd, na.rm = T),
                   
                   max_coverage = max(Coverage, na.rm = T),
                   min_coverage = min(Coverage, na.rm = T),
                   mean_coverage = mean(Coverage, na.rm = T),
                   sd_coverage = sd(Coverage, na.rm = T),
                   
                   
                   max_hf = max(Hf, na.rm = T),
                   min_hf = min(Hf, na.rm = T),
                   mean_hf = mean(Hf, na.rm = T),
                   sd_hf = sd(Hf, na.rm = T),
                   
                   max_lf = max(Lf, na.rm = T),
                   min_lf = min(Lf, na.rm = T),
                   mean_lf = mean(Lf, na.rm = T),
                   sd_lf = sd(Lf, na.rm = T),
                   
                   max_ratioF = max(ratioF, na.rm = T),
                   min_ratioF = min(ratioF, na.rm = T),
                   mean_ratioF = mean(ratioF, na.rm = T),
                   sd_ratioF = sd(ratioF, na.rm = T),
                   nrec= n()) %>% 
  dplyr::ungroup()

# hrv_df_summaries <- arrow::open_dataset('./hrv_metrics/') %>% dplyr::collect()

hrv_df_summaries_480 <- hrv_df_summaries %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(keyCol = paste0(ParticipantIdentifier, Date, collapse = '-')) %>% 
  dplyr::ungroup() %>% 
  dplyr::filter(keyCol %in% weartime_df$keyCol) %>% 
  dplyr::select(-keyCol)

# write.csv(hrv_df_summaries, 'hrv_metrics_daily_summaries.csv')
arrow::write_parquet(hrv_df_summaries, 'hrv_metrics/hrv_metrics_daily_summaries.parquet', chunk_size = 10000000)

arrow::write_parquet(hrv_df_summaries_480, 'hrv_metrics/hrv_metrics_daily_summaries_480.parquet', chunk_size = 10000000)

tictoc::toc()


## filter out to those days with atleast 480min of weartime (from dailydata)

############
# Weekly summaries from daily summaries
############
# aa <- hrv_df_summaries %>% dplyr::filter(ParticipantIdentifier==ParticipantIdentifier[2201])


## USE 480min weartime filtered data
weekly_hrv_summaries <- WeeklyHRVSummaries(hrv_df_summaries_480)
output <- weekly_hrv_summaries %>% 
  dplyr::select(ParticipantIdentifier,
                Date,
                hrv_rmssd = mean_rmssd,
                hrv_coverage = mean_coverage,
                hf = mean_hf,
                lf = mean_lf,
                ratioF = mean_ratioF,
                summary_metric,
                nrec)
  

## get means spread out
output_means <- output %>% 
  dplyr::filter(summary_metric == 'mean') %>% 
  tidyr::gather(concept, nval_num,-ParticipantIdentifier, -Date, -summary_metric) %>% 
  dplyr::mutate(modified_concept = paste0('summary:weekly:',summary_metric,':',tolower(concept))) %>% 
  dplyr::filter(!modified_concept %in% c('summary:weekly:mean:nrec')) %>% 
  dplyr::select(-concept,-summary_metric) %>% 
  dplyr::rename(concept = modified_concept)

## get total(sums) spread out
output_numrecords <- output %>% 
  dplyr::filter(summary_metric == 'total') %>% 
  dplyr::mutate(summary_metric = 'numrecords') %>% 
  tidyr::gather(concept, nval_num,-ParticipantIdentifier, -Date, -summary_metric) %>% 
  dplyr::mutate(modified_concept = paste0('summary:weekly:',summary_metric,':',tolower(concept))) %>% 
  dplyr::filter(modified_concept %in% c('summary:weekly:numrecords:nrec')) %>% 
  dplyr::select(-concept,-summary_metric) %>% 
  dplyr::rename(concept = modified_concept) %>% 
  dplyr::mutate(concept = 'summary:weekly:numrecords:hrv')


output_final <- output_means %>% 
  # dplyr::full_join(output_totals) %>%  # don't need totals(sums)
  dplyr::full_join(output_numrecords)


write.csv(output_final, 'weekly_hrv_summaries_480_mins_cutoff.csv')

f <- 
  synapser::synStore(
    synapser::File(
      path = "weekly_hrv_summaries_480_mins_cutoff.csv",
      parent = 'syn66049173'
    )
  )

