# Airflow ELT DAG Factory for Chaotic Google Sheets

A **config-driven, schema-adaptive ELT pipeline** built with **Apache Airflow** and **BigQuery** to ingest unstable Google Sheets reliably into a data warehouse.

This project demonstrates how to handle real-world spreadsheet chaos — schema drift, inconsistent data types, and undocumented changes — without brittle schema-first ETL pipelines.

> **Related article:**
> [When Google Sheets Break Your ETL: Building a Dynamic Airflow ELT Pipeline with a DAG Factory](https://www.google.com/search?q=Place_your_Medium_link_here)

---

## Why This Project Exists

Google Sheets are widely used as operational data sources, but they break most ETL assumptions:

* **Columns change** without notice.
* **Datatypes** are inconsistent.
* **Validation rules** are implicit.
* **Schemas** are undocumented.

This repository demonstrates a production-grade ELT pattern that:

* **Never fails ingestion** due to schema drift.
* **Infers schema dynamically** at runtime.
* **Pushes validation and transformation** into BigQuery SQL.
* **Scales** using a single DAG factory.
* **Treats invalid data** as first-class output.

---

## High-Level Architecture

```mermaid
graph TD
    A[Google Sheets] --> B[GSheet → GCS → BQ TMP (STRING only)]
    B --> C[Schema Inference (Airflow)]
    C --> D[BigQuery SQL ELT]
    D --> D1[SRC - validated rows]
    D --> D2[REJ - datatype & duplicate rejects]
    D --> D3[STG - typed & normalized]
    D --> D4[DW - deduplicated warehouse table]

```

* **Airflow** orchestrates execution.
* **BigQuery** performs all heavy data processing.

---

## Repository Structure

```text
airflow_elt_dag_factory/
├── dag_factory.py
├── dag_config/
│   └── retail_purchase_demo.yaml
├── operators/
│   ├── gsheet_to_bq.py
│   ├── infer_schema.py
│   └── cleanup_xcom.py
├── sql/
│   ├── ext_to_src.sql
│   ├── rej_datatype.sql
│   ├── rej_duplicate.sql
│   ├── src_to_stg.sql
│   └── stg_to_dw.sql
├── script/
│   └── generate_config.py
└── README.md

```

### Design Principles:

* **Python:** Orchestration & metadata.
* **SQL:** Validation, typing, and deduplication.
* **YAML:** Pipeline configuration.
* **Google Sheets:** Data source + control plane.

---

## Core Concepts

### 1. DAG Factory (Config-Driven Pipelines)

Each pipeline is defined by a YAML file. The DAG factory loads all configs and dynamically registers DAGs. Adding a new pipeline requires **zero** new DAG code.

### 2. Google Sheets as a Control Plane

A `pipeline_control` worksheet defines:

* Job name & Schedule
* Source range & Destination tables
* Primary keys
* Status (OK, DISABLED) — *Only jobs marked OK generate DAG configs.*

### 3. Runtime Schema Inference

Schema is inferred **after** ingestion, not before:

1. Sample data from the `TMP` table.
2. Classify columns (date, numeric, string).
3. Push results to XCom.
4. SQL templates consume inferred metadata.
This avoids schema brittleness entirely.

### 4. SQL-Centric ELT

All validation and transformation happens in BigQuery:

* Regex-based datatype validation.
* Safe casting (`SAFE.PARSE_DATE`, `SAFE_CAST`).
* Deduplication using window functions.
* **Python never processes row-level data.**

### 5. Rejections as First-Class Data

Invalid rows are written to rejection tables with:

* Rejection reason/detail.
* Job metadata.
This enables auditing and data quality feedback loops.

---

## DAG Execution Flow

1. **`gsheet_to_bq`**: Google Sheets → GCS → BigQuery TMP (All STRING).
2. **`infer_schema`**: Infer column definitions and datatype groups.
3. **`ext_to_src`**: Validate rows and load clean data to `SRC`.
4. **`rej_datatype`**: Capture datatype validation failures.
5. **`src_to_stg`**: Apply safe type casting and normalization.
6. **`rej_duplicate`**: Capture duplicate records using business keys.
7. **`stg_to_dw`**: Write final deduplicated warehouse table.
8. **`cleanup_xcom`**: Remove per-run XCom metadata.

---

## Generating a Pipeline Config

Pipeline YAML files are generated from Google Sheets using a CLI script.

### Usage

```bash
python script/generate_config.py \
  --project-id personal-project-dev-482704 \
  --gsheet-id <CONTROL_SHEET_ID> \
  --job-name retail_purchase_demo

```

### Example YAML Configuration (`dag_config/retail_purchase_demo.yaml`)

```yaml
project_id: personal-project-dev-482704
job_name: retail_purchase_demo
schedule: "@daily"

dag_params:
  field_delimiter: '|'

gsheet:
  gsheet_id: <sheet-id>
  range: Purchases_2024!A1:Z

gcs:
  bucket: gsheet_landing
  folder: retail

bq_tables:
  tmp: tmp.retail_purchase_tmp
  src: src.retail_purchase_src
  stg: stg.retail_purchase_stg
  dw: dw.retail_purchase_dw
  rej: rej.retail_purchase_rej

bq_schema:
  primary_key:
    - po_number
    - branch_id
    - transaction_date

```

---

## Requirements

* **Apache Airflow 2.x**
* **Google Cloud Project** with BigQuery & Google Sheets API enabled.
* **Service Account Permissions:**
* BigQuery Job User & Data Editor
* GCS Object Admin
* Sheets Read Access



---

## What This Project Is (and Is Not)

| ✅ This project is: | ❌ This project is NOT: |
| --- | --- |
| A real, runnable pipeline | A generic Airflow tutorial |
| A production-grade ELT pattern | A schema-enforced ETL example |
| A reference for schema-adaptive ingestion | A toy demo |

---

## License

MIT