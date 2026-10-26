
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    count,
    countDistinct,
    sum as spark_sum,
    round as spark_round
)

EVENTS_PATH = "/Volumes/shopstream/core/events/orders_stream"
SCHEMA_PATH = "/Volumes/shopstream/core/events/lakeflow_schema"

# BRONZE: Ingest JSON files using Auto Loader
@dp.table(name="pipeline_job_orders_bronze")
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .schema("""
            event_id STRING,
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            unit_price DOUBLE,
            status STRING,
            event_ts STRING
        """)
        .load(EVENTS_PATH)
        .withColumn("ingestion_ts", current_timestamp())
    )
    


# SILVER: Clean and validate the order events
@dp.table(name="pipeline_job_orders_silver")
@dp.expect("valid_event_id", "event_id IS NOT NULL")
@dp.expect("valid_quantity", "quantity > 0")
@dp.expect("valid_price", "unit_price >= 0")
def orders_silver():
    return (
        spark.readStream
        .table("pipeline_job_orders_bronze")
        .withColumn("event_ts", to_timestamp(col("event_ts")))
        .filter(col("status").isin("completed", "cancelled"))
        .filter(col("event_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") >= 0)
    )


# GOLD: Aggregated order metrics
@dp.materialized_view(name="pipeline_job_order_metrics")
def order_metrics():
    return (
        spark.read.table("pipeline_job_orders_silver")
        .groupBy("status")
        .agg(
            count("*").alias("total_events"),
            countDistinct("order_id").alias("unique_orders"),
            spark_round(
                spark_sum(col("quantity") * col("unit_price")), 2
            ).alias("total_sales_value")
        )
    )
