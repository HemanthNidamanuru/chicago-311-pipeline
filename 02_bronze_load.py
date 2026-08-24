# Databricks notebook source
result = spark.sql("""
    SELECT
      COUNT(*) AS total_records,
      SUM(CASE WHEN sr_number IS NULL THEN 1 ELSE 0 END) AS null_sr_number,
      SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) AS null_created_date,
      COUNT(*) - COUNT(DISTINCT sr_number) AS duplicate_count
    FROM read_files('/Volumes/chicago_311/bronze/raw_landing/311_chicago_*.json', format => 'json')
""")

result.show()

# COMMAND ----------

row = result.collect()[0]

total_records = row["total_records"]
null_sr_number = row["null_sr_number"]
null_created_date = row["null_created_date"]
duplicate_count = row["duplicate_count"]

if total_records == 0:
    raise Exception("Validation failed: no records found in today's file. Nothing to merge.")

warnings = []

if null_sr_number > 0:
    warnings.append(f"{null_sr_number} records have a null sr_number")

if null_created_date > 0:
    warnings.append(f"{null_created_date} records have a null created_date")

if duplicate_count > 0:
    warnings.append(f"{duplicate_count} duplicate sr_number(s) found in source file")

if warnings:
    print("WARNING - data quality issues found in today's file:")
    for w in warnings:
        print(f"  - {w}")
    print("Proceeding to merge anyway (all rows, including flagged ones, will be loaded into bronze).")
else:
    print(f"Validation passed: {total_records} records, no nulls, no duplicates.")

print("Proceeding to merge.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS chicago_311.bronze.service_requests
# MAGIC AS
# MAGIC SELECT * FROM read_files('/Volumes/chicago_311/bronze/raw_landing/311_backfill_2026-08-15.json', format => 'json')
# MAGIC WHERE 1=0;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC MERGE INTO chicago_311.bronze.service_requests AS target
# MAGIC USING (
# MAGIC   SELECT * FROM (
# MAGIC     SELECT *,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY sr_number ORDER BY last_modified_date DESC) AS row_num
# MAGIC     FROM read_files('/Volumes/chicago_311/bronze/raw_landing/311_chicago_*.json', format => 'json')
# MAGIC   )
# MAGIC   WHERE row_num = 1
# MAGIC ) AS source
# MAGIC ON target.sr_number = source.sr_number
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

import os

VOLUME_PATH = "/Volumes/chicago_311/bronze/raw_landing"
PROCESSED_PATH = "/Volumes/chicago_311/bronze/raw_landing/processed"

# Create the processed folder if it doesn't exist
dbutils.fs.mkdirs(PROCESSED_PATH)

# Find all daily files sitting in raw_landing (not already in processed)
files = dbutils.fs.ls(VOLUME_PATH)
daily_files = [f for f in files if f.name.startswith("311_chicago_") and f.name.endswith(".json")]

for f in daily_files:
    destination = f"{PROCESSED_PATH}/{f.name}"
    dbutils.fs.mv(f.path, destination)
    print(f"Archived: {f.name}")

print(f"Total files archived: {len(daily_files)}")