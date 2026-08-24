# Databricks notebook source
from datetime import datetime, timedelta, timezone

right_now = datetime.now(timezone.utc)
yesterday = right_now - timedelta(days=1)

start_of_day = yesterday.strftime("%Y-%m-%d") + "T00:00:00.000"
end_of_day = yesterday.strftime("%Y-%m-%d") + "T23:59:59.999"

# COMMAND ----------

created_row = spark.sql(f"""
    SELECT COUNT(*) AS created_count
    FROM chicago_311.silver.service_requests
    WHERE created_date >= '{start_of_day}' AND created_date <= '{end_of_day}'
""").collect()[0]
created_count = created_row["created_count"]

modified_row = spark.sql(f"""
    SELECT COUNT(*) AS modified_count
    FROM chicago_311.silver.service_requests
    WHERE last_modified_date >= '{start_of_day}' AND last_modified_date <= '{end_of_day}'
      AND NOT (created_date >= '{start_of_day}' AND created_date <= '{end_of_day}')
""").collect()[0]
modified_count = modified_row["modified_count"]

top_types_df = spark.sql(f"""
    SELECT sr_type, COUNT(*) AS type_count
    FROM chicago_311.silver.service_requests
    WHERE created_date >= '{start_of_day}' AND created_date <= '{end_of_day}'
    GROUP BY sr_type
    ORDER BY type_count DESC
    LIMIT 5
""").collect()

top_wards_df = spark.sql(f"""
    SELECT ward, COUNT(*) AS ward_count
    FROM chicago_311.silver.service_requests
    WHERE created_date >= '{start_of_day}' AND created_date <= '{end_of_day}'
      AND ward != 'Unknown'
    GROUP BY ward
    ORDER BY ward_count DESC
    LIMIT 5
""").collect()

print(f"Created yesterday: {created_count}")
print(f"Modified yesterday (existing requests only): {modified_count}")
for row in top_types_df:
    print(f"  {row['sr_type']}: {row['type_count']}")
for row in top_wards_df:
    print(f"  Ward {row['ward']}: {row['ward_count']}")

# COMMAND ----------

import requests as http_requests

report_date_display = yesterday.strftime("%B %d, %Y")
report_date = yesterday.strftime("%Y-%m-%d")

max_type_count = max(row["type_count"] for row in top_types_df)
max_ward_count = max(row["ward_count"] for row in top_wards_df)

rank_colors = ["#1f4e63", "#2c6e91", "#4a8ba8", "#6ba3bd", "#8fbdd1"]

def build_rank_rows(data, label_fn, value_fn, max_value):
    rows = ""
    for i, row in enumerate(data):
        pct = int((value_fn(row) / max_value) * 100)
        color = rank_colors[i] if i < len(rank_colors) else rank_colors[-1]
        rows += f"""
        <tr>
          <td style="padding: 10px 0; width: 28px; vertical-align: middle;">
            <table cellpadding="0" cellspacing="0"><tr>
              <td width="22" height="22" style="background-color: {color}; border-radius: 11px; text-align: center; vertical-align: middle; color: #ffffff; font-size: 11px; font-weight: 700;">{i+1}</td>
            </tr></table>
          </td>
          <td style="padding: 10px 10px; font-size: 13px; color: #23282e; width: 34%; vertical-align: middle;">{label_fn(row)}</td>
          <td style="padding: 10px 0; width: 38%; vertical-align: middle;">
            <table cellpadding="0" cellspacing="0" width="100%" style="background-color: #eef1f3; border-radius: 3px;">
              <tr><td style="background-color: {color}; height: 10px; border-radius: 3px; width: {pct}%;"></td><td></td></tr>
            </table>
          </td>
          <td style="padding: 10px 0 10px 14px; font-size: 13px; font-weight: 700; color: #23282e; text-align: right; width: 16%; vertical-align: middle;">{value_fn(row):,}</td>
        </tr>
        """
    return rows

top_types_rows_html = build_rank_rows(top_types_df, lambda r: r["sr_type"], lambda r: r["type_count"], max_type_count)
top_wards_rows_html = build_rank_rows(top_wards_df, lambda r: f"Ward {r['ward']}", lambda r: r["ward_count"], max_ward_count)

email_html = f"""
<html>
<body style="margin: 0; padding: 0; background-color: #eef1f3; font-family: 'Segoe UI', Helvetica, Arial, sans-serif; color: #23282e;">
  <table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding: 36px 16px;">
  <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 6px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.08);">

    <tr><td style="background-color: #14202b; padding: 22px 32px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td valign="middle">
          <img src="https://cdn.jsdelivr.net/gh/HemanthNidamanuru/chicago-311-pipeline/communityLogo.png" alt="Chicago 311" style="max-width: 120px; height: auto; display: block;" />
        </td>
        <td valign="middle" align="right">
          <p style="color: #8fa3b3; font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px; margin: 0 0 3px 0;">Daily Report</p>
          <p style="color: #ffffff; font-size: 15px; font-weight: 600; margin: 0;">{report_date_display}</p>
        </td>
      </tr></table>
    </td></tr>

    <tr><td style="padding: 26px 32px 22px 32px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td width="50%" style="padding: 16px 20px; background-color: #f4f6f7; border-radius: 5px 0 0 5px; border-right: 1px solid #e6e9eb;">
            <p style="font-size: 10px; text-transform: uppercase; letter-spacing: 0.6px; color: #8a919a; margin: 0 0 6px 0; font-weight: 600;">New Requests</p>
            <p style="font-size: 28px; font-weight: 700; color: #14202b; margin: 0;">{created_count:,}</p>
          </td>
          <td width="50%" style="padding: 16px 20px; background-color: #f4f6f7; border-radius: 0 5px 5px 0;">
            <p style="font-size: 10px; text-transform: uppercase; letter-spacing: 0.6px; color: #8a919a; margin: 0 0 6px 0; font-weight: 600;">Updated Requests</p>
            <p style="font-size: 28px; font-weight: 700; color: #14202b; margin: 0;">{modified_count:,}</p>
          </td>
        </tr>
      </table>
    </td></tr>

    <tr><td style="padding: 6px 32px 8px 32px;">
      <p style="font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.6px; color: #14202b; margin: 0 0 6px 0;">Top New Request Types</p>
    </td></tr>
    <tr><td style="padding: 0 32px 22px 32px;">
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse: collapse;">
        {top_types_rows_html}
      </table>
    </td></tr>

    <tr><td style="padding: 0 32px 8px 32px; border-top: 1px solid #eef1f3; padding-top: 20px;">
      <p style="font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.6px; color: #14202b; margin: 0 0 6px 0;">Top Wards (New Requests)</p>
    </td></tr>
    <tr><td style="padding: 0 32px 26px 32px;">
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse: collapse;">
        {top_wards_rows_html}
      </table>
    </td></tr>

    <tr><td style="padding: 16px 32px; background-color: #f4f6f7; border-top: 1px solid #e6e9eb;">
      <p style="font-size: 11px; color: #9aa1a8; margin: 0; line-height: 1.5;">
        Generated automatically by chicago_311_daily_pipeline. Source: City of Chicago Open Data Portal.
      </p>
    </td></tr>

  </table>
  </td></tr></table>
</body>
</html>
"""

print("HTML email built.")

SENDGRID_API_KEY = dbutils.secrets.get(scope="chicago-311-secrets", key="sendgrid-api-key")
FROM_EMAIL = dbutils.secrets.get(scope="chicago-311-secrets", key="from-email")
TO_EMAIL = dbutils.secrets.get(scope="chicago-311-secrets", key="to-email")

payload = {
    "personalizations": [{"to": [{"email": TO_EMAIL}]}],
    "from": {"email": FROM_EMAIL, "name": "Chicago 311 Pipeline"},
    "subject": f"Chicago 311 Daily Report: {report_date}",
    "content": [{"type": "text/html", "value": email_html}]
}

headers = {
    "Authorization": f"Bearer {SENDGRID_API_KEY}",
    "Content-Type": "application/json"
}

response = http_requests.post("https://api.sendgrid.com/v3/mail/send", json=payload, headers=headers)

if response.status_code == 202:
    print("Email sent successfully!")
else:
    print(f"Failed to send email: {response.status_code} - {response.text}")

# COMMAND ----------

