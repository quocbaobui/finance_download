import argparse
import logging
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from .config import GCS_BUCKET_NAME, GCS_DESTINATION_PATH, LOG_FILE
from .logger import setup_logging

logger = setup_logging(LOG_FILE)

# Define schema for CSV files
SCHEMA = StructType([
    StructField("Comm", StringType(), False),
    StructField("Contract_Type", StringType(), False),
    StructField("Mth_Code", StringType(), False),
    StructField("Year", IntegerType(), False),
    StructField("Strike", FloatType(), False),
    StructField("Trade_Date", DateType(), False),
    StructField("Log_Time", IntegerType(), False),
    StructField("Price", FloatType(), False),
    StructField("Msg_Code", StringType(), True),  # Nullable
    StructField("Volume", IntegerType(), False)
])

def setup_spark() -> SparkSession:
    """Initialize Spark session with GCS connector."""
    try:
        spark = (SparkSession.builder
                 .appName("SGX ETL to ClickHouse")
                 .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22")
                 .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                 .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                 .config("spark.driver.memory", "8g")  # Allocate 8GB for driver
                 .config("spark.executor.memory", "4g")  # Allocate 4GB per executor
                 .config("spark.sql.shuffle.partitions", "100")  # Increase partitions for large data
                 .getOrCreate())
        logger.info("Spark session initialized")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {e}")
        sys.exit(1)

def extract_from_gcs(spark: SparkSession, gcs_path: str) -> "DataFrame":
    """Read all CSV files from GCS with predefined schema."""
    try:
        logger.info(f"Reading CSV files from gs://{GCS_BUCKET_NAME}/{gcs_path}")
        df = (spark.read
              .schema(SCHEMA)
              .option("header", "true")
              .option("mode", "PERMISSIVE")
              .csv(f"gs://{GCS_BUCKET_NAME}/{gcs_path}"))
        logger.info(f"Loaded {df.count()} rows from GCS with {df.rdd.getNumPartitions()} partitions")
        return df.repartition(100)  # Repartition for parallel processing
    except Exception as e:
        logger.error(f"Error reading from GCS: {e}")
        sys.exit(1)

def transform_data(df: "DataFrame") -> "DataFrame":
    """Apply transformations to ensure data consistency."""
    try:
        # Ensure correct data types (already defined in SCHEMA)
        df_transformed = (df
                          .withColumn("Comm", col("Comm").cast("string"))
                          .withColumn("Contract_Type", col("Contract_Type").cast("string"))
                          .withColumn("Mth_Code", col("Mth_Code").cast("string"))
                          .withColumn("Year", col("Year").cast("integer"))
                          .withColumn("Strike", col("Strike").cast("float"))
                          .withColumn("Trade_Date", col("Trade_Date").cast("date"))
                          .withColumn("Log_Time", col("Log_Time").cast("integer"))
                          .withColumn("Price", col("Price").cast("float"))
                          .withColumn("Msg_Code", col("Msg_Code").cast("string"))
                          .withColumn("Volume", col("Volume").cast("integer")))
        logger.info("Data transformations applied")
        return df_transformed
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        sys.exit(1)

def load_to_clickhouse(df: "DataFrame", table: str, batch_size: int = 100000) -> None:
    """Load data into ClickHouse using HTTP interface in batches."""
    try:
        # ClickHouse HTTP endpoint
        clickhouse_url = f"http://localhost:8123/?query=INSERT%20INTO%20{table}%20FORMAT%20CSV"
        logger.info(f"Preparing to load data to ClickHouse table {table}")

        # Process each partition in batches
        def write_batch(iterator):
            batch = []
            for row in iterator:
                # Convert row to CSV format
                csv_row = ','.join(['' if v is None else str(v) for v in row])
                batch.append(csv_row)
                if len(batch) >= batch_size:
                    # Send batch to ClickHouse
                    response = requests.post(clickhouse_url, data='\n'.join(batch))
                    if response.status_code != 200:
                        logger.error(f"Error inserting batch to ClickHouse: {response.text}")
                    batch = []
            # Send remaining batch
            if batch:
                response = requests.post(clickhouse_url, data='\n'.join(batch))
                if response.status_code != 200:
                    logger.error(f"Error inserting final batch to ClickHouse: {response.text}")

        # Apply write_batch to each partition
        df.rdd.foreachPartition(write_batch)
        logger.info(f"Successfully loaded data to ClickHouse table {table}")
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {e}")
        sys.exit(1)

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser  parser = argparse.ArgumentParser(description="ETL pipeline from GCS to ClickHouse using PySpark.")
    parser.add_argument('--gcs-path', default=f"{GCS_DESTINATION_PATH}*.csv",
                        help="GCS path pattern to CSV files (default: sgx-data/*.csv)")
    parser.add_argument('--table', default="sgx_tick_data",
                        help="ClickHouse table name (default: sgx_tick_data)")
    parser.add_argument('--batch-size', type=int, default=100000,
                        help="Number of rows per batch for ClickHouse insertion (default: 100000)")
    return parser.parse_args()

def main():
    """Main ETL function."""
    args = parse_arguments()
    
    # Initialize Spark
    spark = setup_spark()
    
    try:
        # ETL process
        df = extract_from_gcs(spark, args.gcs_path)
        df_transformed = transform_data(df)
        load_to_clickhouse(df_transformed, args.table, args.batch_size)
    finally:
        # Clean up
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
