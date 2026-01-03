import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --------------------------------------------------
# Logging setup (CRITICAL for streaming)
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("spark-consumer")

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaToDelta")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
        "io.delta:delta-spark_2.13:4.0.0"
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)


spark.sparkContext.setLogLevel("INFO")

logger.info("Spark session started")

# --------------------------------------------------
# Kafka Source
# --------------------------------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "coffeehouse_orders")   # ✅ FIXED
    .option("startingOffsets", "latest")
    .load()
)

logger.info("Kafka stream initialized")

# --------------------------------------------------
# Parse JSON
# --------------------------------------------------
raw_orders = df.select(col("value").cast("string").alias("json_data"))

order_details_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("order_details", ArrayType(order_details_schema), True),
    StructField("branch", StringType(), True),
    StructField("total_order_amount", IntegerType(), True),
    StructField("mode_of_payment", StringType(), True)
])

parsed_orders = (
    raw_orders
    .select(from_json(col("json_data"), order_schema).alias("data"))
    .select("data.*")
)

exploded = (
    parsed_orders
    .withColumn("item", explode("order_details"))
    .select(
        "order_id",
        "timestamp",
        "branch",
        "total_order_amount",
        col("item.item_id").alias("item_id"),
        col("item.item_name").alias("item_name"),
        col("item.price").alias("price"),
        col("item.quantity").alias("quantity"),
        col("item.amount").alias("item_amount")
    )
)

logger.info("Transformation pipeline ready")

# --------------------------------------------------
# DEBUG STREAM (TEMPORARY – REMOVE LATER)
# --------------------------------------------------
debug_query = (
    exploded.writeStream
    .format("console")
    .option("truncate", "false")
    .start()
)

# --------------------------------------------------
# Delta Lake Sink
# --------------------------------------------------
raw_container_path = "abfss://raw@himalayanjavadl.dfs.core.windows.net/orders"

def log_batch(batch_df, batch_id):
    count = batch_df.count()
    logger.info(f"Batch {batch_id} received with {count} records")

query = (
    exploded.writeStream
    .foreachBatch(log_batch)   # ✅ LOGS EACH BATCH
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "abfss://raw@himalayanjavadl.dfs.core.windows.net/orders/_checkpoint")
    .option("path", raw_container_path)
    .trigger(processingTime="5 seconds")
    .start()
)

logger.info("Streaming query started")

query.awaitTermination()
