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
