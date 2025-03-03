########
##[RMHDR-271] Compute Cardio Measure Summaries with Minimum Wear Time Restrictions
########

########
## Required Functions/libraries
########
source("scripts/etl/fetch-data.R")
library(synapser)
library(tidyverse)

#### Required functions
CardioScoreMean <- function(CardioScore){
  cardioscore1 <- stringr::str_split(CardioScore,'-')[[1]][1]
  cardioscore2 <- stringr::str_split(CardioScore,'-')[[1]][2] 
  
  cardioscore1 <- as.numeric(cardioscore1)
  cardioscore2 <- as.numeric(cardioscore2)
  
  cardioscoremean <- mean(c(cardioscore1, cardioscore2), na.rm = T)
  return(cardioscoremean)
}

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

WeeklyCardioSummaries <- function(dat) {
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
  out_mean <- data.frame(matrix(NA, nrec, 18))
  out_sum <- data.frame(matrix(NA, nrec, 12))
  out_nrecords <- data.frame(matrix(NA, nrec, 18))
  variable_names <- c(
    "MinsFairlyActive",            
    "MinsLightlyActive",           
    "MinsSedentary",               
    "MinsVeryActive",              
    "Steps",                       
    "BreathingRate",               
    "SpO2_Avg",                    
    "SpO2_Max",                    
    "SpO2_Min",                    
    "RestingHeartRate",            
    "Hrv_DailyRmssd",              
    "Hrv_DeepRmssd",               
    "CombinedActiveMinutes",       
    "MetsOverDay",                 
    'HeartRateIntradayMinuteCount',                    
    "CardioScore"      
  )
  
  # count nDays in the week; nHeartRateIntradayMinuteCount
  variable_names_sum <- c(
    'weeklydayidentity', # ndays in the week
    "MinsFairlyActive",            
    "MinsLightlyActive",           
    "MinsSedentary",               
    "MinsVeryActive",              
    "Steps",                       
    "CombinedActiveMinutes",       
    "MetsOverDay",                 
    'HeartRateIntradayMinuteCount',                    
    "CardioScore"     
    )
  
  # count n records;
  variable_names_nrecords <- c(
    "MinsFairlyActive",            
    "MinsLightlyActive",           
    "MinsSedentary",               
    "MinsVeryActive",              
    "Steps",                       
    "BreathingRate",               
    "SpO2_Avg",                    
    "SpO2_Max",                    
    "SpO2_Min",                    
    "RestingHeartRate",            
    "Hrv_DailyRmssd",              
    "Hrv_DeepRmssd",               
    "CombinedActiveMinutes",       
    "MetsOverDay",                 
    'HeartRateIntradayMinuteCount',                    
    "CardioScore"      
  )
  
  names(out_sum) <- c("ParticipantIdentifier", 
                  "Date", 
                  variable_names_sum)
  
  names(out_mean) <- c("ParticipantIdentifier", 
                       "Date", 
                       variable_names)
  
  names(out_nrecords) <- c("ParticipantIdentifier", 
                           "Date", 
                           variable_names_nrecords)
  
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
    
    ### nRecords
    
    out_nrecords[i, "ParticipantIdentifier"] <- sdat[1, "ParticipantIdentifier"]
    
    ## we set Date to the week's start date (always a Sunday)
    out_nrecords[i, "Date"] <- as.character(sdat[1, "week_startdate"][[1]])
    
    
    out_nrecords[i, variable_names_nrecords] <- apply(sdat[, variable_names_nrecords], 2, my_num_records)
    
  }
  
  out_mean <- out_mean %>% 
    dplyr::mutate(summary_metric = 'mean')
  
  out_sum <- out_sum %>% 
    dplyr::mutate(summary_metric = 'total')
  
  out_nrecords <- out_nrecords %>% 
    dplyr::mutate(summary_metric = 'numrecords')
  
  out <- out_mean %>% 
    dplyr::full_join(out_sum) %>% 
    dplyr::full_join(out_nrecords)
  
  return(out)
}


WeeklyHRSummaries <- function(dat) {
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
  out_mean <- data.frame(matrix(NA, nrec, 3))
  out_sum <- data.frame(matrix(NA, nrec, 3))
  variable_names <- c(
 'AverageHeartRate'      
  )
  
  # count nDays in the week; nHeartRateIntradayMinuteCount
  variable_names_sum <- c(
    'weeklydayidentity' # nrecords
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

############
# Get Data
#############
## Cardio Measures from fitbit daily data
fitbit_dailydata <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "dailydata"))
  )


## Filter to HeartRateIntradayMinuteCount >= 480
cardio_measures <- fitbit_dailydata %>% 
  # dplyr::slice_head(n=10000) %>% 
  # dplyr::filter(ParticipantIdentifier == 'RA11601-00602') %>% 
  dplyr::select(ParticipantIdentifier,
                Date,
                HeartRateIntradayMinuteCount,
                MinsFairlyActive = Tracker_MinutesFairlyActive,
                MinsLightlyActive = Tracker_MinutesLightlyActive,
                MinsSedentary = Tracker_MinutesSedentary,
                MinsVeryActive = Tracker_MinutesVeryActive,
                Steps,
                BreathingRate,
                SpO2_Avg,
                SpO2_Max,
                SpO2_Min,
                RestingHeartRate,
                Hrv_DailyRmssd,
                Hrv_DeepRmssd,
                CardioScore) %>% 
  dplyr::collect() %>% 
  dplyr::mutate(HeartRateIntradayMinuteCount = as.numeric(HeartRateIntradayMinuteCount)) %>% 
  dplyr::filter(HeartRateIntradayMinuteCount >= 480) %>% # HeartRateIntradayMinuteCount > 0, >= 480
  dplyr::mutate(MinsFairlyActive = as.numeric(MinsFairlyActive)) %>% 
  dplyr::mutate(MinsLightlyActive = as.numeric(MinsLightlyActive)) %>% 
  dplyr::mutate(MinsSedentary = as.numeric(MinsSedentary)) %>% 
  dplyr::mutate(MinsVeryActive = as.numeric(MinsVeryActive)) %>% 
  dplyr::mutate(CombinedActiveMinutes = MinsVeryActive + MinsFairlyActive) %>% 
  dplyr::mutate(Date = as.Date.character(Date))  %>% 
  dplyr::mutate(BreathingRate = as.numeric(BreathingRate)) %>%
  dplyr::mutate(SpO2_Avg = as.numeric(SpO2_Avg)) %>%
  dplyr::mutate(SpO2_Max = as.numeric(SpO2_Max)) %>%
  dplyr::mutate(SpO2_Min = as.numeric(SpO2_Min)) %>%
  dplyr::mutate(Hrv_DailyRmssd = as.numeric(Hrv_DailyRmssd)) %>%
  dplyr::mutate(Hrv_DeepRmssd= as.numeric(Hrv_DeepRmssd)) %>%
  dplyr::mutate(Steps = as.numeric(Steps)) %>% 
  dplyr::mutate(RestingHeartRate = as.numeric(RestingHeartRate)) %>% 
  dplyr::filter(!(Steps == 0))  # Steps are non-zero


# cardio score is a mix of range/numbers. convert it to a single number, using mean
cm <- lapply(cardio_measures$CardioScore, function(x){
  CardioScoreMean(x)
}) %>% unlist()

cardio_measures$CardioScoreMean <- cm

## remove the original cardioscore and replace it with mean
cardio_measures <- cardio_measures %>% 
  dplyr::select(-CardioScore) %>% 
  dplyr::rename(CardioScore = CardioScoreMean) %>% 
  unique()

## Apply QA/QC based on https://sagebionetworks.jira.com/browse/RMHDR-242
## Remove only 0 Steps rows, rest keep them but don't include NA in means
# aa_old <- cardio_measures
cardio_measures <- cardio_measures %>% 
  dplyr::rowwise() %>% # Apply QC filters and assgin NA to all vals out of range
  dplyr::mutate(MinsLightlyActive = ifelse(MinsLightlyActive > 1440 || MinsLightlyActive < 0, NA, MinsLightlyActive)) %>% # Mins active caps out at 1440
  dplyr::mutate(MinsFairlyActive = ifelse(MinsFairlyActive > 1440 || MinsFairlyActive < 0, NA, MinsFairlyActive)) %>% # Mins active caps out at 1440
  dplyr::mutate(MinsSedentary = ifelse(MinsSedentary > 1440 || MinsSedentary < 0, NA, MinsSedentary)) %>% # Mins active caps out at 1440
  dplyr::mutate(MinsVeryActive = ifelse(MinsVeryActive > 1440 || MinsVeryActive < 0, NA, MinsVeryActive)) %>% # Mins active caps out at 1440
  dplyr::mutate(BreathingRate = ifelse(BreathingRate > 40 || BreathingRate < 4, NA, BreathingRate)) %>% 
  dplyr::mutate(RestingHeartRate = ifelse(RestingHeartRate > 300 || RestingHeartRate < 25, NA, RestingHeartRate)) %>% 
  dplyr::mutate(SpO2_Avg = ifelse(SpO2_Avg > 100 || SpO2_Avg < 50, NA, SpO2_Avg)) %>% 
  dplyr::mutate(SpO2_Min = ifelse(SpO2_Min > 100 || SpO2_Min < 50, NA, SpO2_Min)) %>% 
  dplyr::mutate(SpO2_Max = ifelse(SpO2_Max > 100 || SpO2_Max < 50, NA, SpO2_Max)) %>% 
  dplyr::mutate(Hrv_DailyRmssd = ifelse(Hrv_DailyRmssd < 3 || Hrv_DailyRmssd > 210, NA, Hrv_DailyRmssd)) %>% 
  dplyr::ungroup()
  # dplyr::filter(BreathingRate >=4, BreathingRate <= 40 ) %>%
  # dplyr::filter(SpO2_Avg >= 50, SpO2_Avg <= 100) %>% 
  # dplyr::filter(SpO2_Max >= 50, SpO2_Max <= 100) %>% 
  # dplyr::filter(SpO2_Min >= 50, SpO2_Min <= 100) %>% 
  # dplyr::filter(Hrv_DailyRmssd >= 3, Hrv_DailyRmssd <= 210) 
  
# no filter for Deep Rmssd, Combined active minutes or Cardio Score

## Get METS from fitbit intraday table
fitbit_intradaycombined <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "intradaycombined"))
  )

## fitbit data is too big; Needs to be broken down into chunks
# Get number of rows per participant in the given dataset(dataset_fitbitintradaycombined)
participant_ids <- fitbit_intradaycombined %>%
  dplyr::filter(Type == 'activities-calories') %>% 
  dplyr::group_by(ParticipantIdentifier) %>%
  dplyr::count() %>% 
  dplyr::collect() %>% 
  as.data.frame()
# %>% 
# dplyr::arrange(n) # ascending order => participant with least number of rows comes first

MAX_ROWS_PER_CHUNK <- 100000000 
# 100 Million rows per chunk. 
# Four participants have above 40Million rows each, the next have around 10Million and less, and so on. 
# Hence instead of treating it as participants per chunk, we will pick the participants
# based on number of rows in a chunk (as it keeps the chunk size approx same)
participant_ids$n_cumsum <- cumsum(as.numeric(participant_ids$n))
participant_ids$batch <- as.integer(participant_ids$n_cumsum/MAX_ROWS_PER_CHUNK)
participant_ids_chunks <- split(participant_ids, participant_ids$batch)
## Basically creates a subset of the dataset containing all the following participants
## Reduce this number if you hit RAM limits, it will increase compute time as we will now have
## more partitions, and for each partition we have to traverse the whole dataset to filter data
## NOTE: If you get into lot many partitions, try increasing the instance. Use memory optimized
## instances like r6a.4x(128GB) - this should be enough, r6a.8X (256GB memory)[this is best]

print(paste0('Total number of chunks is ', length(participant_ids_chunks)))
current_chunk <- 1


# Deal with data in chunks, so as to be easier on RAM
for(current_participant_chunk in participant_ids_chunks){
  print(paste0('Current chunk is ', current_chunk))
  
  temp_df <- fitbit_intradaycombined %>% 
    dplyr::filter(Type == 'activities-calories') %>% 
    dplyr::filter(ParticipantIdentifier %in% current_participant_chunk$ParticipantIdentifier) %>% 
    dplyr::collect()
  
  temp_df <- temp_df %>% 
    dplyr::mutate(Date = as.Date.character(DateTime)) %>% 
    dplyr::mutate(Mets = as.numeric(Mets)) %>% 
    dplyr::group_by(ParticipantIdentifier, Date) %>% 
    dplyr::summarise(MetsOverDay = sum(Mets, na.rm = T),
                     nRecMets = n()) %>% 
    dplyr::ungroup() 
  
  file_name <- paste0('temp_df_mets_',current_chunk,'.csv')
  write.csv(temp_df, file = file_name)
  
  current_chunk <- current_chunk+1
  
  gc()
}

## get all the temp_df_mets files into one dataframe
df_mets_files <- list.files('.') # list of all files
df_mets_files <- df_mets_files[grepl('temp_df_mets',df_mets_files)] # subset to all files that have 'temp_df_mets' in filename
df_mets <- lapply(df_mets_files, function(x){
  read.csv(x,row.names = NULL)
}) %>% data.table::rbindlist(fill = T) %>% 
  dplyr::select(-X) %>% 
  as.data.frame() %>% 
  dplyr::mutate(Date = as.Date.character(Date)) %>% 
  unique()

## AverageHeartRate from activitylogs (this would be avghr during an activity, for eg., walk, run, aerobic exercise etc.,)
fitbit_activitylogs <- 
  arrow::open_dataset(
    s3$path(stringr::str_subset(dataset_paths, "activitylogs"))
  )

df_avghr <- fitbit_activitylogs %>% 
  dplyr::select(ParticipantIdentifier, 
                StartDate,
                EndDate,
                AverageHeartRate) %>% 
  dplyr::collect() %>% 
  dplyr::mutate(Date = as.Date.character(StartDate)) %>% 
  dplyr::mutate(weeklydayidentity = 1) %>% 
  dplyr::mutate(AverageHeartRate = as.numeric(AverageHeartRate)) %>% 
  dplyr::filter(AverageHeartRate != 0) %>% 
  dplyr::filter(AverageHeartRate >= 25) %>% 
  dplyr::filter(AverageHeartRate <= 300) %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(weeklydayidentity = ifelse(is.na(AverageHeartRate),NA,weeklydayidentity)) %>% 
  dplyr::ungroup()

########
# Cardio measures per day/ per week
########
# per day/ daily
cardio_measures_day <- cardio_measures %>% 
  dplyr::left_join(df_mets) %>% 
  dplyr::mutate(MinsFairlyActive = as.numeric(MinsFairlyActive),
                MinsLightlyActive = as.numeric(MinsLightlyActive),
                MinsSedentary = as.numeric(MinsSedentary),
                MinsVeryActive = as.numeric(MinsVeryActive),
                Steps = as.numeric(Steps),
                BreathingRate = as.numeric(BreathingRate),
                SpO2_Avg = as.numeric(SpO2_Avg),
                SpO2_Max = as.numeric(SpO2_Max),
                SpO2_Min = as.numeric(SpO2_Min),
                RestingHeartRate = as.numeric(RestingHeartRate),
                Hrv_DailyRmssd = as.numeric(Hrv_DailyRmssd),
                Hrv_DeepRmssd = as.numeric(Hrv_DeepRmssd),
                CombinedActiveMinutes = as.numeric(CombinedActiveMinutes),
                MetsOverDay = as.numeric(MetsOverDay),
                nRecMets = as.numeric(nRecMets),
                CardioScore = as.numeric(CardioScore)) %>% 
  dplyr::mutate(weeklydayidentity = 1) %>% 
  dplyr::mutate(keyRow = paste0(ParticipantIdentifier,':', Date))


# per week/ weekly
outdat <- WeeklyCardioSummaries(cardio_measures_day) 

## filter to only those days present in cardio measures day (these are the days with HeartRateIntradayMinuteCount >= 480)
df_avghr <- df_avghr %>% 
  dplyr::mutate(keyRow = paste0(ParticipantIdentifier,':',Date)) %>% 
  dplyr::filter(keyRow %in% cardio_measures_day$keyRow) 

outdat_avghr <- WeeklyHRSummaries(df_avghr) 
# unlike cardio measures or Mets, we can have multiple records per day, so summarizing them differently
# 
# i2b2 <- synapser::synGet('syn52562472')$path %>% data.table::fread()
# aa <- i2b2 %>% 
#   dplyr::filter(grepl('avghr',concept)) %>% 
#   dplyr::filter(grepl('weekly', concept)) %>% 
#   dplyr::filter(concept %in% c('mhp:summary:weekly:mean:avghr',
#                                'mhp:summary:weekly:numrecords:avghr'))
# 
# aa_n <- aa %>% 
#   # dplyr::filter(concept == 'mhp:summary:weekly:numrecords:avghr') %>%
#   dplyr::select(ParticipantIdentifier = participantidentifier,
#                 Date = startdate,
#                 enddate,
#                 concept,
#                 nval_num) %>% 
#   dplyr::mutate(Date = as.Date.character(Date)) %>% 
#   dplyr::mutate(nval_num = as.numeric(nval_num))
# 
# aa_n2 <- outdat_avghr %>% 
#   # dplyr::filter(summary_metric == 'total') %>%
#   # dplyr::select(-AverageHeartRate) %>%
#   dplyr::mutate(Date = as.Date.character(Date))
# 
# aa_int <- aa_n %>% 
#   dplyr::inner_join(aa_n2) %>% 
#   dplyr::mutate(diffcol = abs(nval_num-weeklydayidentity)) %>% 
#   dplyr::mutate(diffcol2 = abs(nval_num - AverageHeartRate))
# 
# 
# ## weeks where N does not match
output <- outdat %>% 
  dplyr::select(ParticipantIdentifier,
                Date,
                MinsFairlyActive,
                MinsLightlyActive,
                MinsSedentary,
                MinsVeryActive,
                Steps,
                BreathingRate,
                SpO2_Avg,
                SpO2_Max,
                SpO2_Min,
                RestingHeartRate,
                Hrv_DailyRmssd,
                Hrv_DeepRmssd,
                CombinedActiveMinutes,
                Mets = MetsOverDay,
                CardioScore,
                DayCount = weeklydayidentity,
                HeartRateIntradayMinuteCount,
                summary_metric) %>% 
  dplyr::full_join(outdat_avghr %>% 
                     dplyr::rename(NrecordsAvgHr = weeklydayidentity)) %>% 
  unique()

## get means spread out
output_means <- output %>% 
  dplyr::filter(summary_metric == 'mean') %>% 
  tidyr::gather(concept, nval_num,-ParticipantIdentifier, -Date, -summary_metric) %>% 
  dplyr::mutate(modified_concept = paste0('summary:weekly:',summary_metric,':',tolower(concept))) %>% 
  dplyr::filter(!modified_concept %in% c('summary:weekly:mean:daycount', 
                                         'summary:weekly:mean:nrecordsavghr')) %>% 
  dplyr::select(-concept,-summary_metric) %>% 
  dplyr::rename(concept = modified_concept)

## get numrecords spread out
output_numrecords <- output %>% 
  dplyr::filter(summary_metric == 'numrecords') %>% 
  tidyr::gather(concept, nval_num,-ParticipantIdentifier, -Date, -summary_metric) %>% 
  dplyr::mutate(modified_concept = paste0('summary:weekly:',summary_metric,':',tolower(concept))) %>% 
  dplyr::filter(!modified_concept %in% c('summary:weekly:numrecords:daycount',
                                         'summary:weekly:numrecords:nrecordsavghr',
                                         'summary:weekly:numrecords:averageheartrate')) %>%
  dplyr::select(-concept,-summary_metric) %>% 
  dplyr::rename(concept = modified_concept)

## get total(sums) spread out
output_totals <- output %>% 
  dplyr::filter(summary_metric == 'total') %>% 
  tidyr::gather(concept, nval_num,-ParticipantIdentifier, -Date, -summary_metric) %>% 
  dplyr::mutate(modified_concept = paste0('summary:weekly:',summary_metric,':',tolower(concept))) %>% 
  dplyr::filter(!modified_concept %in% c('summary:weekly:total:daycount', 
                                         'summary:weekly:total:nrecordsavghr')) %>% 
  dplyr::select(-concept,-summary_metric) %>% 
  dplyr::rename(concept = modified_concept)

## get num records only: Records needed not total(sums)
output_numrecords_avghr <- output %>% 
  dplyr::filter(summary_metric == 'total') %>% 
  tidyr::gather(concept, nval_num,-ParticipantIdentifier, -Date, -summary_metric) %>% 
  dplyr::mutate(concept = tolower(concept)) %>% 
  dplyr::filter(concept %in% c(
    # 'daycount',
                               'nrecordsavghr')) %>%
  dplyr::left_join(data.frame(modified_concept = c('minsfairlyactive',
                                                   'minslightlyactive',
                                                   'minssedentary',
                                                   'minsveryactive',
                                                   'steps',
                                                   'breathingrate',
                                                   'spo2_avg',
                                                   'spo2_max',
                                                   'spo2_min',
                                                   'restingheartrate',
                                                   'hrv_dailyrmssd',
                                                   'hrv_deeprmssd',
                                                   'combinedactiveminutes',
                                                   'mets',
                                                   'cardioscore',
                                                   'heartrateintradayminutecount')) %>% 
                     dplyr::mutate(concept = 'daycount')) %>% 
  dplyr::mutate(modified_concept = ifelse(concept == 'daycount',modified_concept,'avghr')) %>% 
  dplyr::mutate(modified_concept = paste0('summary:weekly:numrecords:',modified_concept)) %>% 
  dplyr::select(-concept, -summary_metric) %>% 
  dplyr::rename(concept = modified_concept) 

output_numrecords <- output_numrecords %>% 
  dplyr::full_join(output_numrecords_avghr)

output_final <- output_means %>% 
  # dplyr::full_join(output_totals) %>%  # don't need totals(sums)
  dplyr::full_join(output_numrecords)

### NOTE: for numrecords of mets, we are counting the number of days we have MetsOverDay
###       MetsOverDay is the summation of all Mets from fitbitintradaycombined

write_csv(
  x = output_final, 
  file = file.path(outputDataDir, "weekly_cardio_measures_480_mins_cutoff.csv")
)

f <- 
  synapser::synStore(
    synapser::File(
      path = str_subset(list.files(outputDataDir, full.names = T), 
                        "weekly_cardio_measures_480_mins_cutoff.csv"),
      parent = dailyMeasuresSynDirId
    ), 
    used = parquetDirId
  )
