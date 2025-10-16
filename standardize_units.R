##################
## Code to standardize units across sleep measures
## Current State:
## REM Onset Latency is in sec
## SD of sleep duration is in ms
## SD of midsleep is in hrs
## Next State:
## REM Onset Latency is in sec
## SD of sleep duration is in sec
## SD of midsleep is in sec
##################
library(tidyverse)
library(synapser)

standardizeUnits <- function(df){
  
  # initialize output as a copy of input
  out_df <- df
  
  if('SleepStartTime' %in% colnames(df)){
    out_df <- out_df %>% 
      dplyr::mutate(SleepStartTime = 60*SleepStartTime) # SleepStartTime is in hrs; converting to min
  }
  
  if('SleepEndTime' %in% colnames(df)){
    out_df <- out_df %>% 
      dplyr::mutate(SleepEndTime = 60*SleepEndTime) # SleepEndTime is in hrs; converting to min
  }
  
  if('MidSleep' %in% colnames(df)){
    out_df <- out_df %>% 
      dplyr::mutate(MidSleep = 60*MidSleep) # MidSleep is in hrs; converting to min
  }
  
  if('remOnsetLatency' %in% colnames(df)){
    out_df <- out_df %>% 
      dplyr::mutate(remOnsetLatency = remOnsetLatency/60) # remOnsetLatency is in sec; converting to min
  }
  
 return(out_df)
  
}


writeToDiskAndSynapse <- function(df, file_name, synapseParent){
  
  write_csv(
    x = df, 
    file = file.path(outputDataDirStandard, file_name)
  )
  
  f <- synapser::synStore(
    synapser::File(
      path = str_subset(list.files(outputDataDirStandard, full.names = T), 
                        file_name),
      parent = synapseParent,
      executed = 'https://github.com/itismeghasyam/recover-sleep-measures/blob/main/scripts/standardize_units.R'
    ))
  
}

outputDataDirStandard <- './temp-output-data-standardized'


### Daily measures
## daily_sleep_measures.csv
current_df <- read.csv(synapser::synGet('syn62710530')$path)
standardized_df <- standardizeUnits(current_df)
writeToDiskAndSynapse(standardized_df, 
                      file_name = "daily_sleep_measures_standardized.csv", 
                      synapseParent = 'syn64713363')

######## Sleep SD
#### Duration
## alltime_stats_duration.csv
current_df <- read.csv(synapser::synGet('syn62782793')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(sd = sd/60000) # duration is in ms; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "alltime_stats_duration_standardized.csv", 
                      synapseParent = 'syn64713366')

## alltime_stats_duration_3mo_post_infection.csv
current_df <- read.csv(synapser::synGet('syn62782799')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(sd = sd/60000) # duration is in ms; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "alltime_stats_duration_3mo_post_infection_standardized.csv", 
                      synapseParent = 'syn64713366')

## alltime_stats_duration_6mo_post_infection.csv
current_df <- read.csv(synapser::synGet('syn62782795')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(sd = sd/60000) # duration is in ms; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "alltime_stats_duration_6mo_post_infection_standardized.csv", 
                      synapseParent = 'syn64713366')

## sliding26weeks_stats_duration.csv
current_df <- read.csv(synapser::synGet('syn64139486')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(sd = sd/60000) # duration is in ms; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "sliding26weeks_stats_duration_standardized.csv", 
                      synapseParent = 'syn64713366')

## sliding3weeks_stats_duration.csv
current_df <- read.csv(synapser::synGet('syn64139484')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(sd = sd/60000) # duration is in ms; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "sliding3weeks_stats_duration_standardized.csv", 
                      synapseParent = 'syn64713366')

## weekly_stats_duration.csv
current_df <- read.csv(synapser::synGet('syn62782792')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(sd = sd/60000) # duration is in ms; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "weekly_stats_duration_standardized.csv", 
                      synapseParent = 'syn64713366')

#### MidSleep
## alltime_stats_midsleep.csv
current_df <- read.csv(synapser::synGet('syn62782797')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(circular_sd = circular_sd*60) # midsleep is in hrs; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "alltime_stats_midsleep_standardized.csv", 
                      synapseParent = 'syn64713366')

## alltime_stats_midsleep_3mo_post_infection.csv
current_df <- read.csv(synapser::synGet('syn62782798')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(circular_sd = circular_sd*60) # midsleep is in hrs; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "alltime_stats_midsleep_3mo_post_infection_standardized.csv", 
                      synapseParent = 'syn64713366')

## alltime_stats_midsleep_6mo_post_infection.csv
current_df <- read.csv(synapser::synGet('syn62782796')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(circular_sd = circular_sd*60) # midsleep is in hrs; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "alltime_stats_midsleep_6mo_post_infection_standardized.csv", 
                      synapseParent = 'syn64713366')

## sliding26weeks_stats_midsleep.csv
current_df <- read.csv(synapser::synGet('syn64139488')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(sd = sd*60) # midsleep is in hrs; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "sliding26weeks_stats_midsleep_standardized.csv", 
                      synapseParent = 'syn64713366')

## sliding3weeks_stats_midsleep.csv
current_df <- read.csv(synapser::synGet('syn64139485')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(sd = sd*60) # midsleep is in hrs; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "sliding3weeks_stats_midsleep_standardized.csv", 
                      synapseParent = 'syn64713366')

## weekly_stats_midsleep.csv
current_df <- read.csv(synapser::synGet('syn62782794')$path)
standardized_df <- current_df %>% 
  dplyr::mutate(circular_sd = circular_sd*60) # midsleep is in hrs; converting to min
writeToDiskAndSynapse(standardized_df, 
                      file_name = "weekly_stats_midsleep_standardized.csv", 
                      synapseParent = 'syn64713366')

  