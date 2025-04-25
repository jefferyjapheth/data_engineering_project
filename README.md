# Pyspark Movie Data Analysis Pipeline

## Project Overview
This data engineering project implements a robust data pipeline for movie analysis using Python and Apache Spark. The pipeline extracts data from the TMDB API, processes it through multiple transformation stages, and enables sophisticated analytics on movie performance metrics.

## Architecture & Implementation

### Data Extraction
- Implemented parallel API data extraction using ThreadPoolExecutor to optimize throughput
- Built a robust schema definition system for precise typing of nested JSON structures
- Developed error handling and logging mechanisms to ensure data reliability
- Implemented a caching system using Parquet files to minimize redundant API calls

### Data Processing & Transformation
- Constructed a multi-stage ETL pipeline with clear separation of concerns:
  - Raw data extraction and schema validation
  - Complex nested data extraction (credits, reviews, etc.)
  - Data cleaning and normalization
  - Missing value handling and metric standardization
- Implemented advanced Spark SQL expressions for nested data transformation
- Applied monetary value normalization (converting to millions USD)

### Analytical Features
The pipeline enables several analytical capabilities:

1. **Financial Performance Analysis**
   - Budget vs. revenue comparisons
   - Profit calculations and ROI metrics
   - Identification of financial outliers

2. **Franchise Performance Metrics**
   - Aggregated franchise statistics
   - Performance trends across collections
   - Budget efficiency by franchise

3. **Director Performance Analysis**
   - Revenue generation by director
   - Average ratings across filmography
   - Movie count and consistency metrics

4. **Rating & Popularity Analysis**
   - Vote-weighted rating calculations
   - Popularity vs. critical reception comparisons
   - Audience engagement metrics

## Technical Implementation Details

### Data Structure Design
- Defined comprehensive schema with proper typing for all fields
- Implemented nested structure handling for complex relationships
- Applied transformation logic for arrays, maps, and nested objects

### Performance Optimization
- Utilized parallel processing for API data extraction
- Implemented DataFrame caching for frequently accessed data
- Applied column pruning and reordering for query optimization
- Used Parquet storage for efficient data persistence

### Error Handling & Robustness
- Implemented comprehensive logging throughout the pipeline
- Applied defensive programming techniques for API resilience
- Built data validation checks at multiple processing stages
- Developed graceful fallbacks for missing or corrupt data

## Key Insights & Findings
The pipeline enables extraction of insights such as:

- Identification of highest ROI movies across different budget ranges
- Comparative analysis of franchise performance metrics
- Director success patterns across metrics
- Correlation between popularity metrics and financial performance


## Technologies Used
- Python
- Apache Spark (PySpark)
- TMDB API
- Parquet
- ThreadPoolExecutor
- Pandas/Matplotlib (for visualization)