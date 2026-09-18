# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

# COMMAND ----------

GOLD_TABLES_PATH = "abfss://gold@freightstorageacc.dfs.core.windows.net/delta-tables"
OBT_TABLE        = "freight_catalog.gold.fact_supply_chain_features"
CARRIER_MART     = "freight_catalog.gold.mart_carrier_performance"
LOOKBACK_DAYS    = 7

# COMMAND ----------

shipments = spark.table("freight_catalog.silver.shipment_logs")
weather   = spark.table("freight_catalog.silver.weather")
ports     = spark.table("freight_catalog.silver.port_congestion")
fuel      = spark.table("freight_catalog.silver.fuel_prices")
fbx       = spark.table("freight_catalog.silver.fbx_route_rates")

# COMMAND ----------

default_fuel_benchmark = (
    fuel.select("diesel_national_avg")
    .filter(col("diesel_national_avg").isNotNull())
    .first()
)
fallback_fuel_price = default_fuel_benchmark[0] if default_fuel_benchmark else 3.85

shipments_prep = (
    shipments
    .withColumn("ship_date", to_date(col("transaction_timestamp")))
    .withColumn("ship_week", date_trunc("week", col("transaction_timestamp")))
    .alias("s")
)

fuel_prep = (
    fuel
    .withColumn("fuel_week", date_trunc("week", col("price_date")))
    .dropDuplicates(["fuel_week"])
    .alias("f")
)

ports_prep = (
    ports
    .withColumn("clean_port_name", lower(trim(col("port_name"))))
    .dropDuplicates(["clean_port_name"])
    .alias("p")
)

weather_prep = weather.alias("w")
fbx_prep     = fbx.alias("r")

df_gold_features = (
    shipments_prep
    .join(weather_prep, col("s.ship_date") == col("w.forecast_date"), "left")
    .join(ports_prep, lower(trim(col("s.origin_city"))) == col("p.clean_port_name"), "left")
    .join(fuel_prep, col("s.ship_week") == col("f.fuel_week"), "left")
    .join(fbx_prep,
          (col("s.origin_city") == col("r.origin_port")) &
          (col("s.dest_city")   == col("r.destination_port")),
          "left")
    .select(
        col("s.shipment_id"),
        col("s.ship_date"),
        col("s.transaction_timestamp"),
        col("s.carrier"),
        col("s.origin_city"),
        col("s.dest_city"),
        col("s.commodity"),
        col("s.weight_kg"),
        col("s.delay_hours"),
        when(col("s.delay_hours") > 0, 1).otherwise(0).alias("is_delayed"),
        round(col("s.weight_kg") * 0.05 + col("r.rate_usd_per_teu"), 2).alias("estimated_total_freight_cost"),
        col("w.precipitation_sum_mm"),
        col("w.wind_speed_10m_max_kmh"),
        when(
            (col("w.wind_speed_10m_max_kmh") > 30) |
            (col("w.precipitation_sum_mm") > 10),
            "HIGH"
        ).otherwise("NORMAL").alias("weather_risk_level"),
        coalesce(col("p.congestion_level"), lit("N/A - Inland")).alias("origin_port_congestion"),
        coalesce(col("p.ratio_vs_baseline"), lit(1.00)).alias("port_activity_ratio"),
        coalesce(col("f.diesel_national_avg"), lit(fallback_fuel_price)).alias("eia_diesel_benchmark"),
        current_timestamp().alias("_updated_at")
    )
)

# COMMAND ----------
# First run: create the table partitioned. Later runs: rebuild the rolling window.
# COMMAND ----------

if not spark.catalog.tableExists(OBT_TABLE):
    (df_gold_features.write
        .format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("ship_date")
        .option("path", f"{GOLD_TABLES_PATH}/fact_supply_chain_features")
        .saveAsTable(OBT_TABLE))
    print(f"OBT created: {OBT_TABLE}")
else:
    start = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    end   = (date.today() + timedelta(days=1)).isoformat()

    df_gold_window = df_gold_features.filter(
        f"ship_date >= '{start}' AND ship_date < '{end}'"
    )

    (df_gold_window.write
        .format("delta").mode("overwrite")
        .option("replaceWhere", f"ship_date >= '{start}' AND ship_date < '{end}'")
        .option("mergeSchema", "true")
        .option("path", f"{GOLD_TABLES_PATH}/fact_supply_chain_features")
        .saveAsTable(OBT_TABLE))
    print(f"OBT rebuilt: {start} -> {end}")

# COMMAND ----------

df_carrier_mart = (
    spark.table(OBT_TABLE)
    .groupBy("carrier")
    .agg(
        count("shipment_id").alias("total_shipments"),
        round(avg("delay_hours"), 2).alias("avg_delay_hours"),
        round((sum(when(col("is_delayed") == 0, 1).otherwise(0)) / count("shipment_id")) * 100, 2).alias("otif_on_time_pct"),
        round(sum("weight_kg"), 2).alias("total_tonnage_handled"),
        round(avg("estimated_total_freight_cost"), 2).alias("avg_cost_per_shipment"),
    )
)

(df_carrier_mart.write
    .format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", f"{GOLD_TABLES_PATH}/mart_carrier_performance")
    .saveAsTable(CARRIER_MART))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE freight_catalog.gold.fact_supply_chain_features
# MAGIC WHERE ship_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ZORDER BY (shipment_id, carrier);

# COMMAND ----------

display(spark.table(OBT_TABLE).limit(10))
display(spark.table(CARRIER_MART))