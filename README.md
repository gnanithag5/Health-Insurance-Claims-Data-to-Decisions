# Health-Insurance-Claims-Data-to-Decisions

This project establishes a comprehensive ELT (Extract, Load, Transform) pipeline for healthcare claims data using **Snowflake** as the data warehouse and **dbt** as the primary transformation tool. It is designed to handle and track real-world data changes, including fraud signals, patient conditions, and claim reclassifications.

---

### Pipeline Overview

The pipeline processes data through a structured flow to ensure clean, enriched, and business-ready data:

- **Data Loading**: Raw CSV files are loaded from an **S3 bucket** into the `RAW_DATA` schema in Snowflake.  
- **Seeds**: Static lookup tables (e.g., gender, race, state) are loaded from CSV files into the `LOOKUPS` schema.  
- **Staging Models**: Raw data is cleaned, standardized, and stored in the `STAGING` schema.  
- **Intermediate Models**: Staged data is aggregated and enriched to create a consolidated view in the `INTERMEDIATE` schema.  
- **Marts**: Business-specific fact and dimension tables are built for different use cases:
  - `MARTS_FINANCE`: For financial analysis, including costs and provider utilization.  
  - `MARTS_FRAUD_ANALYSIS`: For detecting fraud patterns and patient utilization.  
- **Snapshots**: dbt snapshots capture and track historical changes in key raw data tables, enabling Slowly Changing Dimensions (SCD) Type 2 tracking.  
- **Simulated Changes**: A script (`simulate_changes.sql`) is provided to update raw data manually, mimicking real-world scenarios and testing SCD tracking functionality.  

---
### Project Structure

- **Data_Loading_S3/**: Contains SQL scripts to load raw data from AWS S3 into Snowflake.  
- **models/staging/**: Cleans and standardizes raw data before moving it into the STAGING schema.  
- **models/intermediate/**: Performs enrichment and aggregation of staged data, creating consolidated datasets in the INTERMEDIATE schema.  
- **models/marts/finance/**: Builds finance-specific marts for analysis of costs, reimbursements, and provider utilization.  
- **models/marts/fraud/**: Builds fraud-focused marts to identify suspicious claims, utilization patterns, and fraud signals.  
- **seeds/**: Holds static lookup tables in CSV format (gender, race, state, yes/no) for reference data.  
- **snapshots/**: Defines dbt snapshot configurations to track historical changes (SCD Type 2).  
- **scripts/**: Includes SQL scripts to simulate real-world data changes for testing snapshots and SCD tracking.  
- **main.py**: Python orchestration script that automates the full pipeline (loading, transformations, snapshots).  
- **config.py**: Loads and manages environment variables such as Snowflake and AWS credentials.  
- **dbt_project.yml**: Main dbt configuration file that defines project settings, models, and dependencies.  
---
### Prerequisites

To run this project, ensure you have:

- Python **3.10+**  
- **dbt-snowflake**  
- A **Snowflake account** with a configured warehouse, role, and database access  
- An **AWS S3 bucket** to stage the raw data  

Install Python dependencies:

```bash
pip install -r requirements.txt 
```

### Running the Pipeline

You have two options to execute the pipeline:

** 1. Manual dbt Commands**

Run each step sequentially:

```bash
dbt seed
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
dbt snapshot
```
## Dashboard
<img width="1417" height="797" alt="Screenshot 2025-09-25 184236" src="https://github.com/user-attachments/assets/ad0b39e5-a3b0-4372-86cb-f534ffe5e2ca" />

<img width="1416" height="797" alt="Screenshot 2025-09-26 124517" src="https://github.com/user-attachments/assets/0974eecb-3915-4e06-a83d-09bfce50cc2d" />
