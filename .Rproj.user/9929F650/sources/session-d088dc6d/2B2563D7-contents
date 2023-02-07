library(tidyverse)
library(httr)
library(jsonlite)
library(DBI)
library(RODBC)
library(odbc)
library(RPostgres)



# Environment Variables, rest of script is triggered by code under functions

#Generalized working directory, change to your own path
setwd("..../Everybody_Votes/")
raw_client_file <- 'vendor_x_data.csv'
deliv_file_name <- "all_vendors.csv"
api_url <- 'https://k4clzaf58d.execute-api.us-east-1.amazonaws.com/default/handle_users'

#Used for DOB to determine if individual was born in 1900s or 2000s
current_year_18_birthyear <- 05

#Pulls API data
get_api_data <- function(api_url){ 
api_data <- as.data.frame(fromJSON(rawToChar(response$content))) 
  return(api_data)
}

#Reads in CSV data
get_csv_data <- function(file_name){
  csv_data <- read_csv(paste0('Data/', file_name))
}

#Merges API and CSV data. Normalizes column types/formatting
merge_data <- function(raw_api_data, raw_csv_data){
  
  colnames(raw_api_data) <- gsub("data.", "", colnames(raw_api_data))
  colnames(raw_csv_data) <- tolower(names(raw_csv_data))
  
  api_data <- raw_api_data %>% 
    rename(vendor_id = "registration_id",
           date_of_birth = "dob",
           email_address = "email") %>% 
    mutate(date_of_birth = ifelse(as.numeric(substr(date_of_birth, 1, 2)) > current_year_18_birthyear, paste0("19",date_of_birth), paste0("20", date_of_birth)),
           date_of_birth = as.Date(date_of_birth, "%Y-%m-%d"),
           citizenship_confirmed = as.character(ifelse(citizenship_confirmed == "true", TRUE, FALSE)),
           home_unit = as.numeric(home_unit),
           mailing_unit = as.numeric(mailing_unit),
           opt_in_to_vendor_email = as.character(ifelse(opt_in_to_vendor_email == "true", TRUE, FALSE)),
           opt_in_to_vendor_sms = as.character(ifelse(opt_in_to_vendor_sms == "true", TRUE, FALSE)),
           opt_in_to_partner_email = as.character(ifelse(opt_in_to_partner_email == "true", TRUE, FALSE)),
           opt_in_to_partner_smsrobocall = as.character(ifelse(opt_in_to_partner_smsrobocall == "true", TRUE, FALSE)),
           volunteer_for_vendor = as.character(ifelse(volunteer_for_vendor == "true", TRUE, FALSE)),
           volunteer_for_partner = as.character(ifelse(volunteer_for_partner == "true", TRUE, FALSE)),
           pre_registered = as.character(ifelse(pre_registered == "true", TRUE, FALSE)),
           registration_date = as.POSIXct(registration_date),
           finish_with_state = as.character(ifelse(finish_with_state == "true", TRUE, FALSE)),
           built_via_api = as.character(ifelse(built_via_api == "true", TRUE, FALSE)),
           submitted_via_state_api = as.character(ifelse(submitted_via_state_api == "true", TRUE, FALSE)),
           shift_id = as.numeric(shift_id),
           shift_type = as.numeric(shift_type),
           vendor_a_shift_id = as.numeric(vendor_a_shift_id),
           has_mailing_address_standardized = as.character(ifelse(has_mailing_address_standardized == "true", TRUE, FALSE)),
           has_state_license_standardized = as.character(ifelse(has_state_license_standardized == "true", TRUE, FALSE)),
           has_ssn_standardized = as.character(ifelse(has_ssn_standardized == "true", TRUE, FALSE)),
           partner_id = as.numeric(partner_id),
           field_start = as.POSIXct(field_start),
           field_end = as.POSIXct(field_start)) %>% 
    select(-results)
  
  csv_data <- raw_csv_data %>% 
    mutate(date_of_birth = as.Date(date_of_birth, "%Y-%m-%d"),
           citizenship_confirmed = as.character(ifelse(citizenship_confirmed == "true", TRUE, FALSE)),
           home_unit = as.numeric(home_unit),
           mailing_unit = as.numeric(mailing_unit),
           opt_in_to_vendor_email = as.character(ifelse(opt_in_to_vendor_email == "true", TRUE, FALSE)),
           opt_in_to_vendor_sms = as.character(ifelse(opt_in_to_vendor_sms == "true", TRUE, FALSE)),
           opt_in_to_partner_email = as.character(ifelse(opt_in_to_partner_email == "true", TRUE, FALSE)),
           opt_in_to_partner_smsrobocall = as.character(ifelse(opt_in_to_partner_smsrobocall == "true", TRUE, FALSE)),
           volunteer_for_vendor = as.character(ifelse(volunteer_for_vendor == "true", TRUE, FALSE)),
           volunteer_for_partner = as.character(ifelse(volunteer_for_partner == "true", TRUE, FALSE)),
           pre_registered = as.character(ifelse(pre_registered == "true", TRUE, FALSE)),
           registration_date = as.POSIXct(registration_date),
           finish_with_state = as.character(ifelse(finish_with_state == "true", TRUE, FALSE)),
           built_via_api = as.character(ifelse(built_via_api == "true", TRUE, FALSE)),
           submitted_via_state_api = as.character(ifelse(submitted_via_state_api == "true", TRUE, FALSE)),
           shift_id = as.numeric(shift_id),
           shift_type = as.numeric(shift_type),
           vendor_a_shift_id = as.numeric(vendor_a_shift_id),
           has_mailing_address_standardized = as.character(ifelse(has_mailing_address_standardized == "true", TRUE, FALSE)),
           has_state_license_standardized = as.character(ifelse(has_state_license_standardized == "true", TRUE, FALSE)),
           has_ssn_standardized = as.character(ifelse(has_ssn_standardized == "true", TRUE, FALSE)),
           partner_id = as.numeric(partner_id),
           field_start = as.POSIXct(field_start),
           field_end = as.POSIXct(field_start))
  
    return(rbind(api_data, csv_data))
}

#updates data in SQL table
updateTable <- function(connection,tn,final_data) {

  dbAppendTable(connection, "vendors", final_data);

}




raw_api_data <- get_api_data(api_url)
raw_csv_data <- get_csv_data(file_name)
final_data <- merge_data(raw_api_data, raw_csv_data)
final_data[is_empty(final_data)] <- NA
connection <- dbConnect(RPostgres::Postgres(), dbname = "sql_pad", host="localhost", port=5432, user="postgres", password="stereo12")
dbGetException(connection)
updateTable(connection, "test", final_data)
write.csv(final_data, paste0("Deliv/", deliv_file_name))
