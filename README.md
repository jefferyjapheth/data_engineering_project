
---

## Pipeline Overview

### Airflow Project: Flight Price Analysis
#### Objective
<p> Develop an end-to-end data pipeline using Apache Airflow to process and analyze flight price
data for Bangladesh. The pipeline must ingest raw CSV data, validate and transform it, compute
key performance indicators (KPIs), and store the final results in a PostgreSQL database for
further analysis. <p>

### 1. Data Ingestion (MySQL Staging)
- **Input**: `Flight_Price_Dataset_of_Bangladesh.csv` (via Kaggle API)
- **Step**: Loaded into a MySQL `staging_db.flight_prices` table
- **Validation**: Ensures correct insertion and schema integrity

### 2. Data Validation (Spark + PySpark)
- Required columns checked: `Airline`, `Source`, `Destination`, `Base Fare`, `Tax & Surcharge`, `Total Fare`
- Handles missing/null values
- Validates data types (e.g., numeric fare values)
- Flags issues like negative fares or blank cities

### 3.Data Transformation & KPI Computation
- Calculates `Total Fare = Base Fare + Tax & Surcharge` if missing
- Computes KPIs using Spark SQL:
  - **Average Fare by Airline**
  - **Seasonal Fare Variation** (e.g., Eid, Winter)
  - **Booking Count by Airline**
  - **Top Routes by Frequency**

### 4.Data Loading into PostgreSQL
- Transformed data written to `flight_price_db` in PostgreSQL
- Dashboards will be powered by PostgreSQL views for KPIs




