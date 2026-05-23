# Adverse Drug Reaction Demographic Pipeline

**A Framework for FDA-Approved Drugs, Clinical Trials, Adverse Events, and Census Data**

*Capstone Project — Nashville Software School Data Engineering | Alex Balli — May 2026*

---

## Overview

This project builds an end-to-end data engineering pipeline that integrates four major public data sources to enable meaningful analysis of adverse drug event demographics. The goal is to create a reusable framework that helps analysts understand who is experiencing adverse drug reactions, how those demographics compare to the broader US population, what related drugs may warrant further investigation, and how clinical trial designs could be improved in the future.

---

## Data Sources

| Source | Description |
|---|---|
| [Drugs@FDA](https://www.fda.gov/drugs/drug-approvals-and-databases/drugsfda-data-files) | FDA-approved drug information |
| [FAERS](https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers) | FDA Adverse Event Reporting System — drug adverse event reports |
| [ClinicalTrials.gov](https://clinicaltrials.gov/) | Clinical trial data |
| [Census.gov](https://www.census.gov/) | US Census demographic data |

---

## System Architecture

The pipeline follows a modern data stack architecture:

```
API Calls & Raw Data Files
        ↓
Python Ingest (async + httpx)
        ↓
AWS S3 (stored as Parquet)
        ↓
Python Load → Snowflake Database
        ↓
dbt (staging and marts tables)
        ↓
Streamlit Dashboard
```

**Orchestration:** Apache Airflow manages scheduling and execution across the pipeline.

**Tech Stack:** Python · AWS S3 · Snowflake · dbt · Apache Airflow · Streamlit · Docker

---

## Ingestion Pipeline Design

Each data source uses a tailored ingestion strategy:

- **ClinicalTrials.gov** — Zip file ingestion
- **Drugs@FDA** — Bulk JSON ingestion
- **FAERS** — Parallel quarterly file ingestion
- **Census.gov** — API calls using `httpx` with async support

---

## Data Modeling (Bronze / Silver / Gold)

The pipeline uses a three-layer data model:

**🟤 Bronze**
Raw drug information, clinical trial data, adverse event data, and census data are loaded directly into S3 as-is.

**⚪ Silver**
Raw tables are cleaned, data types are assigned, necessary derived columns are created (e.g. age group aggregations), and JSON layers are flattened into structured tables.

**🟡 Gold**
Final marts tables are produced by joining tables on shared keys (e.g. drug application number, generic name, or brand name) and selecting the most analytically relevant columns.

---

## Engineering Challenges

- **Blocked ingestion of FAERS and clinical trial data** — navigating access restrictions and format constraints
- **Inconsistent structure across FAERS quarterly files** — schema drift required robust handling across reporting periods
- **Drug name normalization across datasets** — reconciling brand names, generic names, and application numbers across four distinct data sources

---

## What This Framework Seeks to Enable

- Analyzing drug adverse event information by demographic group
- Comparing adverse event demographics against the general US population (via Census data)
- Identifying related drugs that may warrant further safety investigation
- Surfacing insights to improve the design of future clinical trials

---


## Author

**Alex Balli**
Nashville Software School — Data Engineering Cohort (Oct 2025)
Background in chemical engineering and product development, with a focus on data science and data engineering applications in manufacturing and pharmaceutical domains.