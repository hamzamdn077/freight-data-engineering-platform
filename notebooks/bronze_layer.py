# Databricks notebook source
STORAGE_ACCOUNT="freightstorageacc"
CONTAINER_NAME="bronze"
BASE_PATH=f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

df_freight_raw = spark.read.option("multiline", "true").json(f"{BASE_PATH}/freight_rates/*.json")
df_weather_raw = spark.read.option("multiline", "true").json(f"{BASE_PATH}/weather/**/*.json")
df_logs_raw    = spark.read.parquet(f"{BASE_PATH}/*.parquet")

# COMMAND ----------

df_freight_raw.show()
df_weather_raw.show()
df_logs_raw.show()

# COMMAND ----------

from pyspark.sql.functions import *
def enrich_bronze(df):
    return df.withColumn("_ingested_at", current_timestamp()) \
             .withColumn("_source_file", col("_metadata.file_path"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS freight_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS freight_catalog.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS freight_catalog.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS freight_catalog.gold;

# COMMAND ----------

BRONZE_TABLES_PATH=f"{BASE_PATH}/delta-tables"

# COMMAND ----------

enrich_bronze(df_freight_raw).write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .option("path", f"{BRONZE_TABLES_PATH}/freight_rates") \
    .saveAsTable("freight_catalog.bronze.freight_rates")

enrich_bronze(df_weather_raw).write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .option("path", f"{BRONZE_TABLES_PATH}/weather") \
    .saveAsTable("freight_catalog.bronze.weather")

enrich_bronze(df_logs_raw).write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .option("path", f"{BRONZE_TABLES_PATH}/shipment_logs") \
    .saveAsTable("freight_catalog.bronze.shipment_logs")

# COMMAND ----------

