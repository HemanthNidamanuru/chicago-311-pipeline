# Databricks notebook source
import requests
import json
import os
from datetime import datetime, timedelta, timezone

VOLUME_PATH = "/Volumes/chicago_311/bronze/raw_landing"
BASE_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
PAGE_SIZE = 5000

right_now = datetime.now(timezone.utc)
ninety_days_ago = right_now - timedelta(days=90)

start_date_str = ninety_days_ago.strftime("%Y-%m-%dT%H:%M:%S.000")
where_clause = f"created_date >= '{start_date_str}'"

def fetch_page(where_clause, limit, offset):
    params = {
        "$where": where_clause,
        "$limit": PAGE_SIZE,
        "$offset": offset,
    }
    response = requests.get(BASE_URL, params=params)
    return response.json()

all_records = []
offset = 0

while True:
    page = fetch_page(where_clause, PAGE_SIZE, offset)
    if not page:
        break
    all_records.extend(page)
    offset += PAGE_SIZE
    print(f"  fetched so far: {len(all_records)}")
    if len(page) < PAGE_SIZE:
        break

if all_records:
    timestamp = right_now.strftime("%Y-%m-%d")
    filename = f"311_backfill_{timestamp}.json"
    filepath = os.path.join(VOLUME_PATH, filename)
    with open(filepath, "w") as f:
        json.dump(all_records, f)
    print(f"Saved {len(all_records)} records to {filepath}")
else:
    print("No records found.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS chicago_311.bronze.service_requests
# MAGIC AS
# MAGIC SELECT * FROM read_files('/Volumes/chicago_311/bronze/raw_landing/311_backfill_2026-08-15.json', format => 'json')
# MAGIC WHERE 1=0;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO chicago_311.bronze.service_requests AS target
# MAGIC USING (
# MAGIC   SELECT * FROM read_files('/Volumes/chicago_311/bronze/raw_landing/311_backfill_2026-08-15.json', format => 'json')
# MAGIC ) AS source
# MAGIC ON target.sr_number = source.sr_number
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM chicago_311.bronze.service_requests;