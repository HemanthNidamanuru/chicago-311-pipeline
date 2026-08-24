# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_staging AS
# MAGIC WITH ranked AS (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY sr_number ORDER BY last_modified_date DESC) AS row_num
# MAGIC   FROM chicago_311.bronze.service_requests
# MAGIC )
# MAGIC SELECT
# MAGIC   sr_number,
# MAGIC   sr_type,
# MAGIC   sr_short_code,
# MAGIC   owner_department,
# MAGIC   status,
# MAGIC   origin,
# MAGIC   CAST(created_date AS TIMESTAMP) AS created_date,
# MAGIC   CAST(last_modified_date AS TIMESTAMP) AS last_modified_date,
# MAGIC   CAST(closed_date AS TIMESTAMP) AS closed_date,
# MAGIC   street_address,
# MAGIC   city,
# MAGIC   state,
# MAGIC   COALESCE(zip_code, 'Unknown') AS zip_code,
# MAGIC   COALESCE(CAST(ward AS STRING), 'Unknown') AS ward,
# MAGIC   COALESCE(CAST(community_area AS STRING), 'Unknown') AS community_area,
# MAGIC   COALESCE(CAST(police_district AS STRING), 'Unknown') AS police_district,
# MAGIC   CAST(latitude AS DOUBLE) AS latitude,
# MAGIC   CAST(longitude AS DOUBLE) AS longitude,
# MAGIC   CASE WHEN latitude IS NULL OR longitude IS NULL THEN TRUE ELSE FALSE END AS is_missing_location,
# MAGIC   duplicate,
# MAGIC   legacy_record
# MAGIC FROM ranked
# MAGIC WHERE row_num = 1
# MAGIC   AND sr_number IS NOT NULL
# MAGIC   AND created_date IS NOT NULL;

# COMMAND ----------

row = spark.sql("""
    SELECT
      COUNT(*) AS total_rows,
      COUNT(DISTINCT sr_number) AS distinct_ids
    FROM silver_staging
""").collect()[0]

if row["total_rows"] != row["distinct_ids"]:
    raise Exception(f"Validation failed: staging still has duplicates! total={row['total_rows']}, distinct={row['distinct_ids']}")

print(f"Validation passed: {row['total_rows']} rows, all unique sr_numbers. Promoting to real silver table.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE chicago_311.silver.service_requests AS
# MAGIC SELECT * FROM silver_staging;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM chicago_311.silver.service_requests

# COMMAND ----------

