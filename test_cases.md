# E-commerce Streaming Test Cases

This document outlines manual test cases for verifying the functionality of the e-commerce streaming pipeline.

## File Ingestion Tests

### Test 1: Basic File Ingestion
- **Input**: Place a valid CSV file in the `ecommerce_data/` directory
- **Expected**: 
  - Spark job detects and processes the file
  - Console shows batch processing logs
  - File is moved to the `archived/` directory

### Test 2: Multiple File Processing
- **Input**: Place multiple CSV files in the `ecommerce_data/` directory simultaneously
- **Expected**:
  - All files are processed in order
  - Each file appears as a separate batch in logs
  - All files are moved to the `archived/` directory

### Test 3: Empty File Handling
- **Input**: Place an empty CSV file (just headers) in the `ecommerce_data/` directory
- **Expected**:
  - File should be processed without errors
  - No records added to the database
  - File is moved to the `archived/` directory

## Data Transformation Tests

### Test 4: Timestamp Parsing
- **Input**: CSV with various timestamp formats
  - Standard format: `2023-04-15 14:30:45`
  - Different format: `04/15/2023 2:30 PM`
- **Expected**:
  - Standard format parsed correctly
  - Different format shows as null in `event_time`
  - Errors logged but processing continues

### Test 5: Missing Values Handling
- **Input**: CSV with missing values in various columns
- **Expected**:
  - Price: missing values replaced with 0
  - Other columns: null values preserved

### Test 6: UUID Validation
- **Input**: CSV with both valid and invalid UUIDs
- **Expected**:
  - Valid UUIDs are processed normally
  - all UUID columns were cast to uuid
 

## Database Write Tests

### Test 7: Basic DB Write
- **Input**: Standard CSV with complete data
- **Expected**:
  - All records appear in PostgreSQL
  - event_key is correctly generated
  - All fields map to correct database columns

### Test 8: Duplicate Processing Prevention
- **Input**: Process the same file twice
- **Expected**:
  - spark transformation drops duplicates

### Test 9: Large Batch Processing
- **Input**: CSV with 10,000+ records
- **Expected**:
  - All records processed successfully
  - Performance degrades gracefully, not catastrophically

## System Recovery Tests

### Test 10: Checkpoint Recovery
- **Input**: 
  1. Start processing
  2. Force-quit the Spark job mid-processing
  3. Restart the job
- **Expected**:
  - Job picks up where it left off
  - No data duplication occurs
  - Processing continues normally

### Test 11: Database Connection Loss Recovery
- **Input**:
  1. Start processing
  2. Temporarily disconnect PostgreSQL
  3. Reconnect PostgreSQL
- **Expected**:
  - Job logs connection errors
  - Job attempts reconnection
  - Processing resumes after connection restored

## Integration Tests

### Test 12: End-to-End Pipeline
- **Input**: Run data generator and Spark job together
- **Expected**:
  - Data flows from generator to PostgreSQL
  - All processes work together seamlessly

### Test 13: Database Query Performance
- **Input**: Run complex queries on the populated database
- **Expected**:
  - Queries return correct results
  - Performance is acceptable for analytical workloads

### Test 14: Long-Running Stability
- **Input**: Let the system run for 8+ hours with periodic data generation
- **Expected**:
  - Stable performance
  - No memory leaks
  - Consistent processing times
