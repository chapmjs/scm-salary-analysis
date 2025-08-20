# ==========================================
# SIMPLIFIED SCM SALARY ANALYSIS
# Single Occupation: Logisticians (2024)
# ==========================================

# Load required libraries
library(blsAPI)
library(tidyverse)
library(jsonlite)

# Load API key from config file
source("config.R")

# Verify API key
if(Sys.getenv("BLS_KEY") == "") {
  stop("BLS API key not found. Please check your config.R file.")
}

# ==========================================
# CONFIGURATION
# ==========================================

# Single occupation and year
occupation_code <- "13-1081"  # Logisticians
occupation_name <- "Logisticians"
analysis_year <- 2024

cat("Analyzing:", occupation_name, "for year", analysis_year, "\n")

# ==========================================
# DATA COLLECTION
# ==========================================

# Function to construct BLS OEWS series IDs
construct_series_ids <- function(occupation_code) {
  # Remove hyphen and pad to 6 digits
  clean_code <- sprintf("%06s", gsub("-", "", occupation_code))
  
  # OEWS series format: OE + U + N + 0000000 + 000000 + occupation + datatype
  # OE = survey, U = not seasonally adjusted, N = national
  # 0000000 = national area (7 zeros), 000000 = all industries (6 zeros)
  base_id <- paste0("OEUN0000000000000", clean_code)
  
  # Data types: 01=employment, 04=mean_wage, 10=median_wage
  series_ids <- paste0(base_id, c("01", "04", "10"))
  names(series_ids) <- c("employment", "mean_wage", "median_wage")
  
  return(series_ids)
}

# Get data from BLS API
get_salary_data <- function(occupation_code, year) {
  series_ids <- construct_series_ids(occupation_code)
  
  # API payload
  payload <- list(
    'seriesid' = as.vector(series_ids),
    'startyear' = as.character(year),
    'endyear' = as.character(year),
    'registrationKey' = Sys.getenv("BLS_KEY")
  )
  
  # Make API call
  response <- blsAPI(payload, api_version = 2)
  json_data <- fromJSON(response)
  
  # Check if successful
  if(json_data$status != "REQUEST_SUCCEEDED") {
    stop("API request failed: ", json_data$message)
  }
  
  return(json_data)
}

# Fetch the data
cat("Fetching data from BLS API...\n")
raw_data <- get_salary_data(occupation_code, analysis_year)

# ==========================================
# DATA PROCESSING
# ==========================================

# Process API response into clean format
process_data <- function(api_response) {
  cat("Debugging API response structure...\n")
  cat("Response status:", api_response$status, "\n")
  
  # The series data comes as a data.frame, not a list
  series_df <- api_response$Results$series
  cat("Series structure:\n")
  print(str(series_df))
  
  results <- list()
  
  # Handle the data.frame structure
  if(is.data.frame(series_df)) {
    cat("Processing data.frame with", nrow(series_df), "rows\n")
    
    for(i in 1:nrow(series_df)) {
      cat("\n--- Processing row", i, "---\n")
      
      # Extract series information from the row
      if("seriesID" %in% colnames(series_df)) {
        series_id <- series_df$seriesID[i]
      } else {
        cat("No seriesID column found\n")
        print(colnames(series_df))
        next
      }
      
      cat("Series ID:", series_id, "\n")
      
      # Determine data type from series ID ending
      if(grepl("01$", series_id)) {
        data_type <- "employment"
      } else if(grepl("04$", series_id)) {
        data_type <- "mean_wage" 
      } else if(grepl("10$", series_id)) {
        data_type <- "median_wage"
      } else {
        data_type <- "unknown"
      }
      
      cat("Data type:", data_type, "\n")
      
      # Check if there's a data column with the actual time series
      if("data" %in% colnames(series_df)) {
        series_data <- series_df$data[[i]]  # data is likely a list column
        
        if(is.list(series_data) && length(series_data) > 0) {
          cat("Found", length(series_data), "data points\n")
          
          # Look for annual data (M13 period)
          for(point in series_data) {
            if(point$period == "M13") {
              annual_value <- as.numeric(point$value)
              cat("Found annual value:", annual_value, "\n")
              results[[data_type]] <- annual_value
              break
            }
          }
        } else {
          cat("No valid data found in data column\n")
        }
      } else {
        cat("No data column found. Available columns:", paste(colnames(series_df), collapse=", "), "\n")
      }
    }
  } else {
    cat("Unexpected series structure - not a data.frame\n")
    print(class(series_df))
  }
  
  cat("\nFinal results:\n")
  print(results)
  return(results)
}

# Process the data
cat("Processing data...\n")
salary_data <- process_data(raw_data)

# ==========================================
# RESULTS
# ==========================================

# Create summary
create_summary <- function(data, occupation, year) {
  cat("\n", paste(rep("=", 50), collapse=""), "\n")
  cat("SALARY ANALYSIS SUMMARY\n")
  cat(paste(rep("=", 50), collapse=""), "\n")
  cat("Occupation:", occupation, "\n")
  cat("Year:", year, "\n")
  cat("Source: Bureau of Labor Statistics (OEWS)\n\n")
  
  cat("EMPLOYMENT & WAGES:\n")
  cat(paste(rep("-", 30), collapse=""), "\n")
  
  if(!is.null(data$employment)) {
    cat("Total Employment:", scales::comma(data$employment), "workers\n")
  }
  
  if(!is.null(data$median_wage)) {
    cat("Median Annual Wage:", scales::dollar(data$median_wage), "\n")
    cat("Median Hourly Wage:", scales::dollar(data$median_wage / 2080), "\n")
  }
  
  if(!is.null(data$mean_wage)) {
    cat("Mean Annual Wage:", scales::dollar(data$mean_wage), "\n")
    cat("Mean Hourly Wage:", scales::dollar(data$mean_wage / 2080), "\n")
  }
  
  # Calculate wage comparison if both available
  if(!is.null(data$median_wage) && !is.null(data$mean_wage)) {
    wage_ratio <- data$mean_wage / data$median_wage
    cat("\nWage Distribution:\n")
    cat(paste(rep("-", 20), collapse=""), "\n")
    cat("Mean/Median Ratio:", sprintf("%.2f", wage_ratio), "\n")
    if(wage_ratio > 1.1) {
      cat("Distribution: Right-skewed (some high earners)\n")
    } else if(wage_ratio < 0.9) {
      cat("Distribution: Left-skewed (wage compression)\n") 
    } else {
      cat("Distribution: Relatively symmetric\n")
    }
  }
  
  cat("\n", paste(rep("=", 50), collapse=""), "\n")
}

# Display results
create_summary(salary_data, occupation_name, analysis_year)

# ==========================================
# OPTIONAL: SAVE RESULTS
# ==========================================

# Save to CSV for further analysis
results_df <- data.frame(
  occupation = occupation_name,
  year = analysis_year,
  employment = salary_data$employment %||% NA,
  median_wage = salary_data$median_wage %||% NA,
  mean_wage = salary_data$mean_wage %||% NA,
  analysis_date = Sys.Date()
)

# Create output directory if it doesn't exist
if(!dir.exists("output")) dir.create("output")

# Save results
write_csv(results_df, "output/logisticians_salary_2024.csv")
cat("Results saved to: output/logisticians_salary_2024.csv\n")

# ==========================================
# QUICK COMPARISON FUNCTION (OPTIONAL)
# ==========================================

# Function to quickly analyze different occupations
quick_analysis <- function(occ_code, occ_name, year = 2024) {
  cat("\nQuick analysis for:", occ_name, "\n")
  
  tryCatch({
    data <- get_salary_data(occ_code, year)
    processed <- process_data(data)
    
    cat("Employment:", scales::comma(processed$employment %||% "N/A"), "\n")
    cat("Median Wage:", scales::dollar(processed$median_wage %||% 0), "\n")
    cat("Mean Wage:", scales::dollar(processed$mean_wage %||% 0), "\n\n")
    
  }, error = function(e) {
    cat("Error:", e$message, "\n\n")
  })
}

# Example: Compare with other SCM occupations
cat("\nCOMPARISON WITH OTHER SCM OCCUPATIONS:\n")
cat(paste(rep("=", 45), collapse=""), "\n")

# Uncomment to compare with other occupations:
# quick_analysis("11-3071", "Transportation Managers")
# quick_analysis("13-1023", "Purchasing Agents")  
# quick_analysis("11-3061", "Purchasing Managers")

cat("Analysis complete!\n")