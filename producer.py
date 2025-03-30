from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from datetime import datetime

# S3 Configuration
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'minioadmin')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'minioadmin')
S3_BUCKET = os.getenv('S3_BUCKET', 'real-estate-data')

# Define schema for the real estate data (from your code)
real_estate_schema = StructType([
    StructField("property_id", StringType()),
    StructField("address", StructType([
        StructField("street", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("zip_code", StringType()),
        StructField("country", StringType())
    ])),
    StructField("property_details", StructType([
        StructField("type", StringType()),
        StructField("transaction_type", StringType()),
        StructField("year_built", IntegerType()),
        StructField("construction_type", StringType()),
        StructField("floors", IntegerType()),
        StructField("lot_size", DoubleType()),
        StructField("living_area", IntegerType()),
        StructField("bedrooms", IntegerType()),
        StructField("bathrooms", DoubleType()),
        StructField("amenities", ArrayType(StringType())),
        StructField("has_garage", BooleanType()),
        StructField("parking_spaces", IntegerType()),
        StructField("neighborhood_class", StringType())
    ])),
    StructField("financials", StructType([
        StructField("price", DoubleType()),
        StructField("price_per_sqft", DoubleType()),
        StructField("tax_assessment", DoubleType()),
        StructField("hoa_fee", DoubleType()),
        StructField("last_sale_price", DoubleType()),
        StructField("last_sale_date", StringType())
    ])),
    StructField("listing_details", StructType([
        StructField("listing_date", StringType()),
        StructField("days_on_market", IntegerType()),
        StructField("listing_status", StringType()),
        StructField("description", StringType())
    ])),
    StructField("agent", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("company", StringType())
    ])),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("school_district", StringType()),
        StructField("walk_score", IntegerType()),
        StructField("transit_score", IntegerType())
    ])),
    StructField("timestamp", StringType())
])


def write_to_s3(batch_df, batch_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    batch_df.write \
        .mode("append") \
        .parquet(f"s3a://{S3_BUCKET}/data/"
                 f"year={datetime.now().year}/"
                 f"month={datetime.now().month}/"
                 f"day={datetime.now().day}/"
                 f"batch_{batch_id}_{timestamp}.parquet")


def process_real_estate_stream():
    """Main streaming processing function"""
    # Initialize Spark with S3 and Kafka support
    spark = SparkSession.builder \
        .appName("RealEstateStreamProcessor") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.streaming.checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints") \
        .getOrCreate()

    # Read from Kafka with enhanced configuration
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092") \
        .option("subscribe", "real-estate-listings") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "10000") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON and apply schema
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), real_estate_schema).alias("data")).select("data.*")

    # Add processing metadata and watermark
    processed_df = parsed_df \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withWatermark("processing_timestamp", "10 minutes")

    # Real-time analytics example: Windowed aggregations
    windowed_analytics = processed_df \
        .groupBy(
        window(col("processing_timestamp"), "15 minutes"),
        col("address.city"),
        col("property_details.type")
    ) \
        .agg(
        avg("financials.price").alias("avg_price"),
        count("*").alias("listing_count"),
        avg("financials.price_per_sqft").alias("avg_price_per_sqft")
    )

    # Write the raw data to S3 in Parquet format
    s3_sink = processed_df.writeStream \
        .foreachBatch(write_to_s3) \
        .outputMode("append") \
        .trigger(processingTime="5 minutes") \
        .start()

    # Write the analytics to console (could also write to another Kafka topic or database)
    analytics_sink = windowed_analytics.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()

    # Wait for termination
    s3_sink.awaitTermination()
    analytics_sink.awaitTermination()


if __name__ == "__main__":
    process_real_estate_stream()