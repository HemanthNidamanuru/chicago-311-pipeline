# Chicago 311 Data Pipeline

A production-style data pipeline that ingests, cleans, and aggregates Chicago's 311 service request data on Databricks. It runs on a daily schedule, applies data quality checks at each stage, and sends an automated summary report by email.

## Architecture

```
Chicago 311 API (Socrata)
        |
        v
Bronze  -  raw data, incrementally merged
        |
        v
Silver  -  deduplicated, typed, validated
        |
        v
Gold    -  aggregated tables for reporting
        |
        +--> Tableau dashboards
        +--> Automated email report (SendGrid)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Compute / storage | Databricks (Delta Lake, Unity Catalog) |
| Transformation | PySpark, Spark SQL |
| Orchestration | Databricks Jobs |
| Secrets management | Databricks Secrets |
| Visualization | Tableau |
| Email delivery | SendGrid API |

## Pipeline Notebooks

| Notebook | Description |
|---|---|
| `00_backfill_extract` | One-time 90-day historical backfill. Run manually, not part of the scheduled job. |
| `01_extract_daily` | Pulls the previous day's records from the Socrata API. |
| `02_bronze_load` | Validates the incoming file, merges it into the bronze table, and archives the file once processed. |
| `03_silver_transform` | Deduplicates on `sr_number`, casts types, handles missing values, and validates the result before promoting it. |
| `04_gold_aggregate` | Builds the aggregated tables used for reporting and dashboards. |
| `05_send_report` | Queries the previous day's activity and emails an HTML summary. |

## Orchestration

The five notebooks run as a single Databricks Job (`chicago_311_daily_pipeline`), scheduled daily at 8:00 AM:

```
extract_daily -> bronze_load -> silver_transform -> gold_aggregate -> send_report
```

Each task depends on the one before it, so the report only sends if the full pipeline succeeds.

## Data Quality Design

- **Bronze** keeps an unmodified copy of whatever the source API returns. Incoming files are checked for nulls and duplicates before merging, but the raw layer is never edited after the fact — cleanup happens downstream.
- **Silver** removes duplicates (keeping the most recently updated record per `sr_number`), casts fields to proper types, and recovers records with partial data instead of dropping them outright. Transformations are built in a staging table first and only promoted to the live table after passing validation.
- **Bronze load is incremental by design.** Early on, the merge step re-scanned every previously processed file on each run, which slowed down over time. This was fixed by archiving each file into a separate folder immediately after a successful merge, so subsequent runs only touch new data.

## Dashboards

Built in Tableau, connected live to the gold layer:
- Requests by ward
- Requests by type
- Resolution rate
- Response time by type

## Automated Reporting

After a successful run, `05_send_report` emails a summary covering new and updated requests, the top request types, and the top wards by volume for the previous day.

## Known Limitations

- The report email is sent from a personal address without domain authentication (SPF/DKIM), so it can occasionally land in spam. A production setup would use an authenticated custom domain.

## Data Source

[City of Chicago 311 Service Requests](https://data.cityofchicago.org/Service-Requests/311-Service-Requests/v6vf-nfxy) — Chicago Data Portal.
