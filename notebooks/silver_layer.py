# Databricks notebook source
from pyspark.sql.functions import (
    col, explode, to_timestamp, to_date, arrays_zip, current_timestamp,
    row_number, round as spark_round, trim, upper, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------
# Config
# COMMAND ----------

STORAGE_ACCOUNT = "freightstorageacc"
SILVER_BASE_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_TABLES_PATH = f"{SILVER_BASE_PATH}/delta-tables"

CATALOG = "freight_catalog"
SCHEMA = "silver"

def full_table_name(t):
    return f"{CATALOG}.{SCHEMA}.{t}"

def table_path(t):
    return f"{SILVER_TABLES_PATH}/{t}"

# COMMAND ----------
# Helper: write a dataframe to silver as an upsert
#
# First run: table doesn't exist yet, so we just write everything
# (mode=overwrite is safe here since there's nothing to blow away).
#
# Every run after: we MERGE on the keys so we only touch rows that
# actually changed. New keys get inserted, existing ones get updated,
# and anything in the target that isn't in the source stays put.
#
# Also dedupes the source before merging - Delta will throw if two
# source rows match the same target row, so we keep the newest one
# (by _ingested_at) and drop the rest.
# COMMAND ----------

def upsert_delta(df_source, table, merge_keys, dedupe_order_col="_ingested_at"):
    target_name = full_table_name(table)
    target_path = table_path(table)

    # keep one row per key - newest ingestion wins
    w = Window.partitionBy(*merge_keys).orderBy(col(dedupe_order_col).desc())
    df_dedup = (
        df_source
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    if spark.catalog.tableExists(target_name):
        # <=> is null-safe equals, so a null key on either side still matches
        merge_condition = " AND ".join(
            [f"t.{k} <=> s.{k}" for k in merge_keys]
        )
        (
            DeltaTable.forName(spark, target_name)
            .alias("t")
            .merge(df_dedup.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[MERGE] {target_name} | source rows: {df_dedup.count()}")
    else:
        (
            df_dedup.write
            .format("delta")
            .mode("overwrite")
            .option("path", target_path)
            .saveAsTable(target_name)
        )
        print(f"[INSERT] {target_name} | rows: {df_dedup.count()}")

# COMMAND ----------
# Read bronze
# COMMAND ----------

df_bronze_freight = spark.read.table(f"{CATALOG}.bronze.freight_rates")
df_bronze_weather = spark.read.table(f"{CATALOG}.bronze.weather")
df_bronze_logs    = spark.read.table(f"{CATALOG}.bronze.shipment_logs")

# COMMAND ----------
# Shipment logs
# Merge key: shipment_id + transaction_timestamp
# COMMAND ----------

df_silver_shipments = (
    df_bronze_logs
    .select(
        col("shipment_id"),
        to_timestamp(col("transaction_date")).alias("transaction_timestamp"),
        upper(trim(col("carrier"))).alias("carrier"),
        upper(trim(col("origin_city"))).alias("origin_city"),
        upper(trim(col("dest_city"))).alias("dest_city"),
        to_timestamp(col("planned_eta")).alias("planned_eta"),
        to_timestamp(col("actual_eta")).alias("actual_eta"),
        col("delay_hours").cast("int").alias("delay_hours"),
        col("weight_kg").cast("double").alias("weight_kg"),
        upper(trim(col("commodity"))).alias("commodity"),
        col("_ingested_at"),
    )
    # can't merge on nulls meaningfully, drop them
    .filter(col("shipment_id").isNotNull())
    .filter(col("transaction_timestamp").isNotNull())
)

upsert_delta(
    df_silver_shipments,
    table="shipment_logs",
    merge_keys=["shipment_id", "transaction_timestamp"],
)

# COMMAND ----------
# FBX route rates
# Merge key: origin_port + destination_port + container_type
# COMMAND ----------

df_silver_fbx = (
    df_bronze_freight
    .select(explode(col("fbx_route_rates")).alias("fbx"), col("_ingested_at"))
    .select(
        upper(trim(col("fbx.origin"))).alias("origin_port"),
        upper(trim(col("fbx.dest"))).alias("destination_port"),
        col("fbx.rate_usd_per_teu").cast("double").alias("rate_usd_per_teu"),
        col("fbx.index").alias("fbx_index"),
        upper(trim(col("fbx.container"))).alias("container_type"),
        col("_ingested_at"),
    )
    .filter(col("origin_port").isNotNull() & col("destination_port").isNotNull())
)

upsert_delta(
    df_silver_fbx,
    table="fbx_route_rates",
    merge_keys=["origin_port", "destination_port", "container_type"],
)

# COMMAND ----------
# Port congestion
# Merge key: locode + as_of_date
# COMMAND ----------

df_silver_port = (
    df_bronze_freight
    .select(explode(col("port_congestion")).alias("pc_item"), col("_ingested_at"))
    .select(explode(col("pc_item.data.data.results")).alias("res"), col("_ingested_at"))
    .select(
        upper(trim(col("res.port"))).alias("port_name"),
        upper(trim(col("res.locode"))).alias("locode"),
        upper(trim(col("res.country"))).alias("country"),
        col("res.congestion").alias("congestion_level"),
        to_date(col("res.as_of")).alias("as_of_date"),
        col("res.metrics.recent_avg_portcalls_per_day").cast("double").alias("recent_avg_portcalls"),
        col("res.metrics.baseline_avg_portcalls_per_day").cast("double").alias("baseline_avg_portcalls"),
        col("res.metrics.ratio_vs_baseline").cast("double").alias("ratio_vs_baseline"),
        col("_ingested_at"),
    )
    .filter(col("locode").isNotNull() & col("as_of_date").isNotNull())
)

upsert_delta(
    df_silver_port,
    table="port_congestion",
    merge_keys=["locode", "as_of_date"],
)

# COMMAND ----------
# Fuel prices
# Merge key: price_date
# COMMAND ----------

df_silver_fuel = (
    df_bronze_freight
    .select(col("fuel_prices.data.data").alias("fuel"), col("_ingested_at"))
    .select(
        to_date(col("fuel.diesel.updated_at")).alias("price_date"),
        col("fuel.diesel.national_average").cast("double").alias("diesel_national_avg"),
        col("fuel.diesel.regions.east_coast").cast("double").alias("diesel_east_coast"),
        col("fuel.diesel.regions.midwest").cast("double").alias("diesel_midwest"),
        col("fuel.diesel.regions.gulf_coast").cast("double").alias("diesel_gulf_coast"),
        col("fuel.diesel.regions.west_coast").cast("double").alias("diesel_west_coast"),
        col("fuel.gasoline.national_average").cast("double").alias("gasoline_national_avg"),
        col("_ingested_at"),
    )
    .filter(col("price_date").isNotNull())
)

upsert_delta(
    df_silver_fuel,
    table="fuel_prices",
    merge_keys=["price_date"],
)

# COMMAND ----------
# Weather
# Merge key: latitude + longitude + forecast_date
#
# lat/lon come in as doubles and floats are annoying to compare with
# equals in a merge, so we round to 4 decimals (~11m precision, way
# more than enough for weather grid points).
# COMMAND ----------

df_weather_zipped = (
    df_bronze_weather
    .select(
        spark_round(col("latitude").cast("double"), 4).alias("latitude"),
        spark_round(col("longitude").cast("double"), 4).alias("longitude"),
        col("elevation"),
        col("timezone"),
        explode(
            arrays_zip(
                col("daily.time"),
                col("daily.precipitation_sum"),
                col("daily.wind_speed_10m_max"),
                col("daily.weather_code"),
            )
        ).alias("weather_day"),
        col("_ingested_at"),
    )
)

df_silver_weather = (
    df_weather_zipped
    .select(
        col("latitude"),
        col("longitude"),
        col("elevation"),
        col("timezone"),
        to_date(col("weather_day.time")).alias("forecast_date"),
        col("weather_day.precipitation_sum").cast("double").alias("precipitation_sum_mm"),
        col("weather_day.wind_speed_10m_max").cast("double").alias("wind_speed_10m_max_kmh"),
        col("weather_day.weather_code").cast("int").alias("wmo_weather_code"),
        col("_ingested_at"),
    )
    .filter(col("forecast_date").isNotNull())
)

upsert_delta(
    df_silver_weather,
    table="weather",
    merge_keys=["latitude", "longitude", "forecast_date"],
)

# COMMAND ----------
# Sanity check - row counts, schema, quick peek
# COMMAND ----------

silver_tables = [
    "fbx_route_rates",
    "port_congestion",
    "fuel_prices",
    "weather",
    "shipment_logs",
]

for t in silver_tables:
    fq = full_table_name(t)
    if spark.catalog.tableExists(fq):
        df = spark.table(fq)
        print(f"TABLE: {fq} | Rows: {df.count()}")
        df.printSchema()
        display(df.limit(10))
    else:
        print(f"Table {fq} does not exist")