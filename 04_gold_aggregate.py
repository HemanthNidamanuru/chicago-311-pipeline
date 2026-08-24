# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE chicago_311.gold.requests_by_ward AS
# MAGIC SELECT
# MAGIC   ward,
# MAGIC   COUNT(*) AS request_count
# MAGIC FROM chicago_311.silver.service_requests
# MAGIC GROUP BY ward
# MAGIC ORDER BY request_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE chicago_311.gold.requests_by_type AS
# MAGIC SELECT
# MAGIC   sr_type,
# MAGIC   COUNT(*) AS request_count
# MAGIC FROM chicago_311.silver.service_requests
# MAGIC GROUP BY sr_type
# MAGIC ORDER BY request_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE chicago_311.gold.response_time_by_type AS
# MAGIC SELECT
# MAGIC   sr_type,
# MAGIC   ROUND(AVG(DATEDIFF(closed_date, created_date)), 2) AS avg_days_to_close,
# MAGIC   COUNT(*) AS total_requests
# MAGIC FROM chicago_311.silver.service_requests
# MAGIC WHERE closed_date IS NOT NULL
# MAGIC GROUP BY sr_type
# MAGIC ORDER BY avg_days_to_close DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE chicago_311.gold.resolution_rate AS
# MAGIC SELECT
# MAGIC   status,
# MAGIC   COUNT(*) AS request_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
# MAGIC FROM chicago_311.silver.service_requests
# MAGIC GROUP BY status
# MAGIC ORDER BY request_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE chicago_311.gold.daily_volume AS
# MAGIC SELECT
# MAGIC   DATE(created_date) AS request_date,
# MAGIC   COUNT(*) AS requests_created,
# MAGIC   SUM(CASE WHEN closed_date IS NULL THEN 1 ELSE 0 END) AS still_open
# MAGIC FROM chicago_311.silver.service_requests
# MAGIC GROUP BY DATE(created_date)
# MAGIC ORDER BY request_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE chicago_311.gold.geo_points AS
# MAGIC SELECT
# MAGIC   sr_type,
# MAGIC   ward,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   status
# MAGIC FROM chicago_311.silver.service_requests
# MAGIC WHERE is_missing_location = FALSE;