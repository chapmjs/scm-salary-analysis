# ==========================================
# COMPREHENSIVE SCM SALARY ANALYSIS
# Multiple Supply Chain Management Occupations
# ==========================================

# Load required libraries
library(blsAPI)
library(tidyverse)
library(jsonlite)
library(scales)

# Load API key from config file
source("config.R")

# Verify API key
if(Sys.getenv("BLS_KEY") == "") {
  stop("BLS API key not found. Please check your config.R file.")
}

# ==========================================
# CONFIGURATION
# ==========================================

# Define Supply Chain Management occupations
# Note: Some codes may not have current data in BLS OEWS
scm_occupations <- list(
  # Management Level
  "11-3061" = "Purchasing Managers",
  "11-3071" = "Transportation, Storage, and Distribution Managers", 
  "11-9199" = "Other Management Occupations",
  
  # Professional/Analytical  
  "13-1081" = "Logisticians",
  "13-1023" = "Purchasing Agents (except Wholesale, Retail, Farm)",
  "13-1022" = "Wholesale and Retail Buyers (except Farm Products)",
  "13-1199" = "Other Business Operations Specialists",
  
  # Specialized/Support
  "43-5061" = "Production, Planning, and Expediting Clerks",
  "43-5071" = "Shipping, Receiving, and Traffic Clerks"
  # Note: 53-1047 (Traffic Technicians) may not have current OEWS data
)

# Alternative/backup occupation codes to try if primary ones fail
backup_occupations <- list(
  "13-1021" = "Buyers and Purchasing Agents, Farm Products",
  "43-5021" = "Couriers and Messengers", 
  "43-5052" = "Postal Service Mail Carriers",
  "53-7064" = "Packers and Packagers, Hand"
)

analysis_year <- 2024

cat("Analyzing", length(scm_occupations), "SCM occupations for year", analysis_year, "\n")
cat("Occupations to analyze:\n")
for(code in names(scm_occupations)) {
  cat("  ", code, "-", scm_occupations[[code]], "\n")
}
cat("\n")

# ==========================================
# CORE FUNCTIONS
# ==========================================

# Function to construct BLS OEWS series IDs
construct_series_ids <- function(occupation_code) {
  # Remove hyphen and pad to 6 digits
  clean_code <- sprintf("%06s", gsub("-", "", occupation_code))
  
  # OEWS series format: OE + U + N + 0000000 + 000000 + occupation + datatype
  base_id <- paste0("OEUN0000000000000", clean_code)
  
  # Data types: 01=employment, 04=mean_wage, 13=annual_median_wage
  series_ids <- paste0(base_id, c("01", "04", "13"))
  names(series_ids) <- c("employment", "mean_wage", "median_wage")
  
  return(series_ids)
}

# Get data from BLS API with error handling
get_occupation_data <- function(occupation_code, year, max_retries = 3) {
  series_ids <- construct_series_ids(occupation_code)
  
  # API payload
  payload <- list(
    'seriesid' = as.vector(series_ids),
    'startyear' = as.character(year),
    'endyear' = as.character(year),
    'registrationKey' = Sys.getenv("BLS_KEY")
  )
  
  # Retry logic for API calls
  for(attempt in 1:max_retries) {
    tryCatch({
      response <- blsAPI(payload, api_version = 2)
      json_data <- fromJSON(response)
      
      if(json_data$status == "REQUEST_SUCCEEDED") {
        return(json_data)
      } else {
        warning(paste("API request failed for", occupation_code, ":", json_data$message))
        if(attempt == max_retries) return(NULL)
      }
    }, error = function(e) {
      warning(paste("Error fetching data for", occupation_code, "on attempt", attempt, ":", e$message))
      if(attempt == max_retries) return(NULL)
      Sys.sleep(1) # Brief pause before retry
    })
  }
  return(NULL)
}

# Process API response into clean format with robust error handling
process_occupation_data <- function(api_response, occupation_code, occupation_name) {
  # Return empty result if no API response
  if(is.null(api_response)) {
    cat("    No API response received\n")
    return(create_empty_result(occupation_code, occupation_name))
  }
  
  # Check if Results exist and have series
  if(is.null(api_response$Results) || is.null(api_response$Results$series)) {
    cat("    No series data in API response\n")
    return(create_empty_result(occupation_code, occupation_name))
  }
  
  series_df <- api_response$Results$series
  
  # Check if series_df is valid
  if(!is.data.frame(series_df) || nrow(series_df) == 0) {
    cat("    Invalid or empty series data\n")
    return(create_empty_result(occupation_code, occupation_name))
  }
  
  results <- list(employment = NA, median_wage = NA, mean_wage = NA)
  
  # Process each series with additional error checking
  for(i in 1:nrow(series_df)) {
    tryCatch({
      series_id <- series_df$seriesID[i]
      
      # Check if data column exists and is a list
      if(!"data" %in% names(series_df) || !is.list(series_df$data)) {
        cat("    Warning: 'data' column missing or invalid for series", series_id, "\n")
        next
      }
      
      series_data <- series_df$data[[i]]
      
      # Multiple checks for series_data validity
      if(is.null(series_data)) {
        cat("    Warning: NULL data for series", series_id, "\n")
        next
      }
      
      if(!is.data.frame(series_data)) {
        cat("    Warning: Non-dataframe data for series", series_id, "\n")
        next
      }
      
      if(nrow(series_data) == 0) {
        cat("    Warning: Empty data for series", series_id, "\n")
        next
      }
      
      if(!"value" %in% names(series_data)) {
        cat("    Warning: No 'value' column for series", series_id, "\n")
        next
      }
      
      # Extract and convert value
      raw_value <- series_data$value[1]
      if(is.na(raw_value) || raw_value == "" || raw_value == "-") {
        cat("    Warning: Invalid value for series", series_id, "\n")
        next
      }
      
      value <- as.numeric(raw_value)
      if(is.na(value)) {
        cat("    Warning: Could not convert value to numeric for series", series_id, "\n")
        next
      }
      
      # Assign value based on series type
      if(grepl("01$", series_id)) {
        results$employment <- value
      } else if(grepl("04$", series_id)) {
        results$mean_wage <- value
      } else if(grepl("13$", series_id)) {
        results$median_wage <- value
      }
      
    }, error = function(e) {
      cat("    Error processing series", i, ":", e$message, "\n")
    })
  }
  
  return(data.frame(
    occupation_code = occupation_code,
    occupation_name = occupation_name,
    employment = results$employment,
    median_wage = results$median_wage,
    mean_wage = results$mean_wage,
    data_available = !all(is.na(c(results$employment, results$median_wage, results$mean_wage)))
  ))
}

# Helper function to create empty results
create_empty_result <- function(occupation_code, occupation_name) {
  return(data.frame(
    occupation_code = occupation_code,
    occupation_name = occupation_name,
    employment = NA,
    median_wage = NA,
    mean_wage = NA,
    data_available = FALSE
  ))
}

# ==========================================
# BATCH DATA COLLECTION
# ==========================================

# Function to analyze all occupations
analyze_all_occupations <- function(occupations_list, year) {
  cat("Starting batch analysis of", length(occupations_list), "occupations...\n")
  
  all_results <- list()
  successful <- 0
  failed <- 0
  
  for(i in seq_along(occupations_list)) {
    code <- names(occupations_list)[i]
    name <- occupations_list[[code]]
    
    cat(sprintf("(%d/%d) Analyzing: %s - %s\n", i, length(occupations_list), code, name))
    
    # Add small delay to be respectful to API
    if(i > 1) Sys.sleep(0.5)
    
    # Get and process data
    raw_data <- get_occupation_data(code, year)
    processed_data <- process_occupation_data(raw_data, code, name)
    
    all_results[[i]] <- processed_data
    
    if(processed_data$data_available) {
      successful <- successful + 1
      cat("  ✓ Success: Employment =", comma(processed_data$employment %||% 0), 
          ", Median =", dollar(processed_data$median_wage %||% 0), "\n")
    } else {
      failed <- failed + 1
      cat("  ✗ Failed: No data available\n")
    }
  }
  
  cat(sprintf("\nBatch analysis complete: %d successful, %d failed\n", successful, failed))
  
  # Combine all results
  final_results <- do.call(rbind, all_results)
  return(final_results)
}

# ==========================================
# DEBUGGING AND TROUBLESHOOTING FUNCTIONS
# ==========================================

# Function to debug a specific occupation code
debug_occupation <- function(occupation_code, year = 2024) {
  cat("DEBUGGING OCCUPATION:", occupation_code, "\n")
  cat(paste(rep("-", 40), collapse=""), "\n")
  
  # Check series ID construction
  series_ids <- construct_series_ids(occupation_code)
  cat("Series IDs constructed:\n")
  for(i in 1:length(series_ids)) {
    cat("  ", names(series_ids)[i], ":", series_ids[i], "\n")
  }
  
  # Test API call
  payload <- list(
    'seriesid' = as.vector(series_ids),
    'startyear' = as.character(year),
    'endyear' = as.character(year),
    'registrationKey' = Sys.getenv("BLS_KEY")
  )
  
  cat("\nMaking API call...\n")
  
  tryCatch({
    response <- blsAPI(payload, api_version = 2)
    json_data <- fromJSON(response)
    
    cat("API Status:", json_data$status, "\n")
    
    if(!is.null(json_data$message)) {
      cat("API Message:", json_data$message, "\n")
    }
    
    if(!is.null(json_data$Results)) {
      cat("Results structure available:", !is.null(json_data$Results$series), "\n")
      
      if(!is.null(json_data$Results$series)) {
        series_df <- json_data$Results$series
        cat("Number of series returned:", nrow(series_df), "\n")
        
        if(nrow(series_df) > 0) {
          cat("Series IDs returned:\n")
          for(i in 1:nrow(series_df)) {
            series_id <- series_df$seriesID[i]
            data_available <- !is.null(series_df$data[[i]]) && 
              is.data.frame(series_df$data[[i]]) && 
              nrow(series_df$data[[i]]) > 0
            cat("  ", series_id, "- Data available:", data_available, "\n")
            
            if(data_available) {
              first_value <- series_df$data[[i]]$value[1]
              cat("    First value:", first_value, "\n")
            }
          }
        }
      }
    }
    
  }, error = function(e) {
    cat("ERROR:", e$message, "\n")
  })
  
  cat(paste(rep("-", 40), collapse=""), "\n")
}

# Function to test all series IDs individually
test_individual_series <- function(occupation_code, year = 2024) {
  cat("TESTING INDIVIDUAL SERIES FOR:", occupation_code, "\n")
  series_ids <- construct_series_ids(occupation_code)
  
  for(i in 1:length(series_ids)) {
    series_name <- names(series_ids)[i]
    series_id <- series_ids[i]
    
    cat("\nTesting", series_name, "(", series_id, "):\n")
    
    payload <- list(
      'seriesid' = series_id,
      'startyear' = as.character(year),
      'endyear' = as.character(year),
      'registrationKey' = Sys.getenv("BLS_KEY")
    )
    
    tryCatch({
      response <- blsAPI(payload, api_version = 2)
      json_data <- fromJSON(response)
      
      if(json_data$status == "REQUEST_SUCCEEDED" && 
         !is.null(json_data$Results$series) &&
         nrow(json_data$Results$series) > 0) {
        
        series_data <- json_data$Results$series$data[[1]]
        if(nrow(series_data) > 0) {
          cat("  ✓ Success - Value:", series_data$value[1], "\n")
        } else {
          cat("  ✗ No data rows\n")
        }
      } else {
        cat("  ✗ Failed -", json_data$message %||% "Unknown error", "\n")
      }
      
    }, error = function(e) {
      cat("  ✗ Error:", e$message, "\n")
    })
  }
}

cat("Fetching data from BLS API...\n")
scm_data <- analyze_all_occupations(scm_occupations, analysis_year)

# ==========================================
# ANALYSIS AND REPORTING
# ==========================================

# Add calculated fields
scm_data <- scm_data %>%
  mutate(
    median_hourly = median_wage / 2080,
    mean_hourly = mean_wage / 2080,
    wage_ratio = mean_wage / median_wage,
    wage_distribution = case_when(
      wage_ratio > 1.15 ~ "Right-skewed (high earners)",
      wage_ratio < 0.85 ~ "Left-skewed (compressed)",
      TRUE ~ "Relatively symmetric"
    ),
    occupation_level = case_when(
      str_detect(occupation_code, "^11-") ~ "Management",
      str_detect(occupation_code, "^13-") ~ "Professional/Analytical", 
      str_detect(occupation_code, "^43-|^53-") ~ "Support/Specialized",
      TRUE ~ "Other"
    )
  ) %>%
  arrange(desc(median_wage))

# ==========================================
# COMPREHENSIVE REPORTING
# ==========================================

# Summary statistics
create_comprehensive_report <- function(data) {
  cat("\n", paste(rep("=", 70), collapse=""), "\n")
  cat("COMPREHENSIVE SCM SALARY ANALYSIS REPORT\n")
  cat(paste(rep("=", 70), collapse=""), "\n")
  cat("Analysis Date:", format(Sys.Date(), "%B %d, %Y"), "\n")
  cat("Data Year:", analysis_year, "\n")
  cat("Source: Bureau of Labor Statistics (OEWS)\n")
  cat("Occupations Analyzed:", nrow(data), "\n")
  cat("Occupations with Data:", sum(data$data_available), "\n\n")
  
  # Filter to available data
  available_data <- data %>% filter(data_available == TRUE)
  
  if(nrow(available_data) == 0) {
    cat("No data available for analysis.\n")
    return()
  }
  
  # Overall SCM market summary
  total_employment <- sum(available_data$employment, na.rm = TRUE)
  weighted_median <- weighted.mean(available_data$median_wage, available_data$employment, na.rm = TRUE)
  
  cat("OVERALL SCM MARKET SUMMARY:\n")
  cat(paste(rep("-", 35), collapse=""), "\n")
  cat("Total SCM Employment:", comma(total_employment), "workers\n")
  cat("Employment-Weighted Median Wage:", dollar(weighted_median), "\n")
  cat("Wage Range:", dollar(min(available_data$median_wage, na.rm=TRUE)), 
      "to", dollar(max(available_data$median_wage, na.rm=TRUE)), "\n\n")
  
  # By occupation level
  cat("ANALYSIS BY OCCUPATION LEVEL:\n")
  cat(paste(rep("-", 35), collapse=""), "\n")
  
  level_summary <- available_data %>%
    group_by(occupation_level) %>%
    summarise(
      count = n(),
      total_employment = sum(employment, na.rm = TRUE),
      median_wage_avg = mean(median_wage, na.rm = TRUE),
      median_wage_range = paste(dollar(min(median_wage, na.rm=TRUE)), 
                                "to", dollar(max(median_wage, na.rm=TRUE))),
      .groups = 'drop'
    )
  
  for(i in 1:nrow(level_summary)) {
    level <- level_summary$occupation_level[i]
    cat(sprintf("%s:\n", level))
    cat(sprintf("  Occupations: %d\n", level_summary$count[i]))
    cat(sprintf("  Employment: %s\n", comma(level_summary$total_employment[i])))
    cat(sprintf("  Avg Median Wage: %s\n", dollar(level_summary$median_wage_avg[i])))
    cat(sprintf("  Wage Range: %s\n\n", level_summary$median_wage_range[i]))
  }
  
  # Top paying occupations
  cat("TOP 5 HIGHEST PAYING SCM OCCUPATIONS:\n")
  cat(paste(rep("-", 45), collapse=""), "\n")
  
  top_5 <- available_data %>% 
    arrange(desc(median_wage)) %>% 
    head(5)
  
  for(i in 1:nrow(top_5)) {
    cat(sprintf("%d. %s\n", i, top_5$occupation_name[i]))
    cat(sprintf("   Median: %s | Mean: %s | Employment: %s\n", 
                dollar(top_5$median_wage[i]),
                dollar(top_5$mean_wage[i]), 
                comma(top_5$employment[i])))
    cat(sprintf("   Distribution: %s\n\n", top_5$wage_distribution[i]))
  }
  
  cat(paste(rep("=", 70), collapse=""), "\n")
}

# Generate the report
create_comprehensive_report(scm_data)

# ==========================================
# DETAILED DATA TABLE
# ==========================================

# Print detailed table
cat("\nDETAILED OCCUPATION DATA:\n")
cat(paste(rep("=", 90), collapse=""), "\n")

detailed_table <- scm_data %>%
  filter(data_available == TRUE) %>%
  select(occupation_code, occupation_name, occupation_level, 
         employment, median_wage, mean_wage, wage_distribution) %>%
  arrange(desc(median_wage))

print(detailed_table)

# ==========================================
# DATA EXPORT
# ==========================================

# Create output directory
if(!dir.exists("output")) dir.create("output")

# Save comprehensive results
write_csv(scm_data, paste0("output/scm_salary_analysis_", analysis_year, ".csv"))

# Save summary by level
level_summary <- scm_data %>%
  filter(data_available == TRUE) %>%
  group_by(occupation_level) %>%
  summarise(
    occupations_count = n(),
    total_employment = sum(employment, na.rm = TRUE),
    median_wage_avg = mean(median_wage, na.rm = TRUE),
    median_wage_min = min(median_wage, na.rm = TRUE),
    median_wage_max = max(median_wage, na.rm = TRUE),
    mean_wage_avg = mean(mean_wage, na.rm = TRUE),
    .groups = 'drop'
  )

write_csv(level_summary, paste0("output/scm_level_summary_", analysis_year, ".csv"))

cat("\nFiles saved:\n")
cat("- output/scm_salary_analysis_", analysis_year, ".csv\n")
cat("- output/scm_level_summary_", analysis_year, ".csv\n")

# ==========================================
# OPTIONAL: COMPARISON FUNCTIONS
# ==========================================

# Function to compare specific occupations
compare_occupations <- function(codes_to_compare) {
  comparison_data <- scm_data %>%
    filter(occupation_code %in% codes_to_compare, data_available == TRUE) %>%
    arrange(desc(median_wage))
  
  cat("\nOCCUPATION COMPARISON:\n")
  cat(paste(rep("-", 40), collapse=""), "\n")
  
  for(i in 1:nrow(comparison_data)) {
    cat(sprintf("%s:\n", comparison_data$occupation_name[i]))
    cat(sprintf("  Median Wage: %s\n", dollar(comparison_data$median_wage[i])))
    cat(sprintf("  Employment: %s\n", comma(comparison_data$employment[i])))
    cat(sprintf("  Level: %s\n\n", comparison_data$occupation_level[i]))
  }
}

# Example comparison - uncomment to use
# compare_occupations(c("13-1081", "11-3061", "13-1023"))

# Function to add custom occupations
add_custom_occupation <- function(code, name) {
  cat("Analyzing custom occupation:", name, "\n")
  
  raw_data <- get_occupation_data(code, analysis_year)
  processed_data <- process_occupation_data(raw_data, code, name)
  
  if(processed_data$data_available) {
    cat("Results for", name, ":\n")
    cat("  Employment:", comma(processed_data$employment), "\n")
    cat("  Median Wage:", dollar(processed_data$median_wage), "\n")
    cat("  Mean Wage:", dollar(processed_data$mean_wage), "\n")
  } else {
    cat("No data available for", name, "\n")
  }
  
  return(processed_data)
}

cat("\n✓ Analysis complete! Check the output folder for detailed results.\n")
