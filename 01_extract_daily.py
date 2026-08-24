# Databricks notebook source
import requests
import json
import os
from datetime import datetime, timedelta, timezone

VOLUME_PATH = "/Volumes/chicago_311/bronze/raw_landing"
BASE_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
PAGE_SIZE = 5000

right_now = datetime.now(timezone.utc)
yesterday = right_now - timedelta(days=1)

start_of_day = yesterday.strftime("%Y-%m-%d") + "T00:00:00.000"
end_of_day = yesterday.strftime("%Y-%m-%d") + "T23:59:59.999"

where_clause = f"(created_date >= '{start_of_day}' AND created_date <= '{end_of_day}') OR (last_modified_date >= '{start_of_day}' AND last_modified_date <= '{end_of_day}')"

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
    if len(page) < PAGE_SIZE:
        break

if all_records:
    timestamp = yesterday.strftime("%Y-%m-%d")
    filename = f"311_chicago_{timestamp}.json"
    filepath = os.path.join(VOLUME_PATH, filename)
    with open(filepath, "w") as f:
        json.dump(all_records, f)
    print(f"Saved {len(all_records)} records to {filepath}")
else:
    print("No new records today.")




# COMMAND ----------

