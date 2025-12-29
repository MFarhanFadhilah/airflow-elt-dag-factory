This project demonstrates a SQL-first ELT pipeline for ingesting schema-chaotic
Google Sheets using Airflow and BigQuery.

Key ideas:
- Treat Google Sheets as untrusted input
- Land all data as STRING
- Enforce quality using BigQuery SQL
- Capture rejects explicitly
- Orchestrate with Airflow, compute in BigQuery
