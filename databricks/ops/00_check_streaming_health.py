dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("lookback_minutes", "60")
dbutils.widgets.text("min_records_expected", "1000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lookback = int(dbutils.widgets.get("lookback_minutes"))
min_records = int(dbutils.widgets.get("min_records_expected"))

# Check IoT streaming ingestion health
count = spark.sql(
    f"""
  SELECT COUNT(*) as cnt
  FROM {catalog}.{schema}.machine_events
  WHERE ts >= current_timestamp() - INTERVAL {lookback} MINUTES
"""
).collect()[0]["cnt"]

print(f"Records in last {lookback} minutes: {count}")

if count < min_records:
    raise Exception(
        f"Streaming health check FAILED: Only {count} records found (expected >= {min_records})"
    )
else:
    print(f"✅ Streaming health check PASSED: {count} records ingested")
