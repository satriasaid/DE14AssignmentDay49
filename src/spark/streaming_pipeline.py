import os
import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum as _sum, avg as _avg,
    when, lit, trim, struct, to_json, current_timestamp, date_format, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
)
from pyspark.sql.window import Window

# -------------------------------------------------------------------------
# Configuration & Environment Variables
# -------------------------------------------------------------------------
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "transactions")
VALID_OUTPUT_TOPIC = os.environ.get("VALID_OUTPUT_TOPIC", "transactions_valid")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "transactions_dlq")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/app/src/checkpoints/streaming_job")

# Global accumulators for tracking cumulative metrics across micro-batches
cumulative_state = {
    "running_total": 0.0,
    "total_valid_count": 0,
    "total_dlq_count": 0,
    "seen_keys": set()  # (user_id, timestamp) for cross-batch deduplication
}


def get_spark_session() -> SparkSession:
    """Initialize and configure SparkSession with Kafka package."""
    return SparkSession.builder \
        .appName("TransactionsRealTimePipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


def get_transaction_schema() -> StructType:
    """
    Transaction JSON Schema:
    {
      "user_id": "U12345",
      "amount": 150000,
      "timestamp": "2025-12-14T09:00:20Z",
      "source": "mobile"
    }
    """
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True),
    ])


def evaluate_batch(batch_df, batch_id):
    """
    Processes each micro-batch:
    1. Evaluates 5 Mandatory Validation Rules & Watermark / Late arrival check.
    2. Identifies Duplicate Transactions (user_id + timestamp).
    3. Adds `is_valid` (boolean) and `error_reason` (string) columns.
    4. Routes valid transactions to Kafka topic `transactions_valid`.
    5. Routes invalid transactions to Kafka topic `transactions_dlq`.
    6. Computes 1-Minute Tumbling Window aggregations.
    7. Updates and prints cumulative `running_total` and `timestamp` to console.
    """
    current_time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if batch_df.isEmpty():
        return

    # Cache DataFrame for multiple downstream actions in this micro-batch
    batch_df.cache()

    # ---------------------------------------------------------------------
    # Step 1: Intra-batch Duplicate Detection using Window Function
    # ---------------------------------------------------------------------
    # Detect duplicates within the same batch (partition by user_id and timestamp)
    window_spec = Window.partitionBy("user_id", "timestamp").orderBy(
        when(col("event_time").isNotNull(), col("event_time")).otherwise(lit("1970-01-01"))
    )
    df_with_rank = batch_df.withColumn("intra_batch_dup_rank", row_number().over(window_spec))

    # ---------------------------------------------------------------------
    # Step 2: Evaluate 5 Mandatory Validation Rules + Watermark/Late Check
    # ---------------------------------------------------------------------
    # Rule 1: Mandatory Field Check (user_id, amount, timestamp, source not null/empty)
    # Rule 2: Type Validation (timestamp parseable to ISO timestamp, amount is valid numeric)
    # Rule 3: Range Validation for Amount (1 <= amount <= 10,000,000)
    # Rule 4: Source Validation (source in ['mobile', 'web', 'pos'])
    # Rule 5: Duplicate Detection (user_id + timestamp) & Late Event check (> 3 mins)
    # ---------------------------------------------------------------------
    watermark_cutoff_seconds = 3 * 60  # 3 minutes watermark tolerance

    validated_df = df_with_rank.withColumn(
        "error_reason",
        # 1. Mandatory Field Checks
        when(col("user_id").isNull() | (trim(col("user_id")) == ""), lit("Missing mandatory field: user_id"))
        .when(col("amount").isNull(), lit("Missing mandatory field: amount"))
        .when(col("timestamp").isNull() | (trim(col("timestamp")) == ""), lit("Missing mandatory field: timestamp"))
        .when(col("source").isNull() | (trim(col("source")) == ""), lit("Missing mandatory field: source"))
        # 2. Type Validations
        .when(col("event_time").isNull(), lit("Type validation failed: invalid/unparseable timestamp"))
        .when(col("amount").isNaN(), lit("Type validation failed: amount is NaN"))
        # 3. Range Validation (1 to 10,000,000)
        .when((col("amount") < 1.0) | (col("amount") > 10000000.0), lit("Range validation failed: amount must be between 1 and 10,000,000"))
        # 4. Source Validation ('mobile', 'web', 'pos')
        .when(~col("source").isin("mobile", "web", "pos"), lit("Source validation failed: source must be mobile, web, or pos"))
        # 5. Duplicate Detection (within batch)
        .when(col("intra_batch_dup_rank") > 1, lit("Duplicate transaction: same user_id and timestamp within batch"))
        # Watermark Late Event Detection (arrived > 3 minutes late compared to processing time)
        .when(
            (current_timestamp().cast("long") - col("event_time").cast("long")) > watermark_cutoff_seconds,
            lit("Watermark violation: event timestamp is > 3 minutes late")
        )
        .otherwise(lit(None).cast(StringType()))
    )

    # Cross-batch deduplication using driver cache:
    # Convert records to check against historical seen keys
    rows = validated_df.collect()
    updated_rows = []
    
    batch_valid_count = 0
    batch_dlq_count = 0
    batch_valid_amount = 0.0

    for r in rows:
        row_dict = r.asDict()
        u_id = row_dict.get("user_id")
        t_stamp = row_dict.get("timestamp")
        err = row_dict.get("error_reason")
        
        # If passed initial validation, check cross-batch seen_keys
        if err is None:
            key = (u_id, t_stamp)
            if key in cumulative_state["seen_keys"]:
                err = "Duplicate transaction: duplicate user_id and timestamp (cross-batch)"
                row_dict["error_reason"] = err
                row_dict["is_valid"] = False
                batch_dlq_count += 1
            else:
                cumulative_state["seen_keys"].add(key)
                # Keep cache bounded to last 10,000 items
                if len(cumulative_state["seen_keys"]) > 10000:
                    cumulative_state["seen_keys"].pop()
                row_dict["is_valid"] = True
                batch_valid_count += 1
                batch_valid_amount += (row_dict.get("amount") or 0.0)
        else:
            row_dict["is_valid"] = False
            batch_dlq_count += 1

        updated_rows.append(row_dict)

    # Update cumulative running metrics
    cumulative_state["running_total"] += batch_valid_amount
    cumulative_state["total_valid_count"] += batch_valid_count
    cumulative_state["total_dlq_count"] += batch_dlq_count

    spark = batch_df.sparkSession
    processed_df = spark.createDataFrame(updated_rows)

    # ---------------------------------------------------------------------
    # Step 3: Route Valid Data -> Kafka Topic: transactions_valid
    # ---------------------------------------------------------------------
    valid_records = processed_df.filter(col("is_valid") == True)
    if not valid_records.isEmpty():
        valid_kafka_payload = valid_records.select(
            when(col("user_id").isNotNull(), col("user_id")).otherwise(lit("UNKNOWN")).alias("key"),
            to_json(struct(
                col("user_id"),
                col("amount"),
                col("timestamp"),
                col("source"),
                col("event_time"),
                col("is_valid"),
                col("error_reason")
            )).alias("value")
        )
        valid_kafka_payload.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", VALID_OUTPUT_TOPIC) \
            .save()

    # ---------------------------------------------------------------------
    # Step 4: Route Invalid Data (DLQ) -> Kafka Topic: transactions_dlq
    # ---------------------------------------------------------------------
    dlq_records = processed_df.filter(col("is_valid") == False)
    if not dlq_records.isEmpty():
        dlq_kafka_payload = dlq_records.select(
            when(col("user_id").isNotNull(), col("user_id")).otherwise(lit("INVALID")).alias("key"),
            to_json(struct(
                col("user_id"),
                col("amount"),
                col("timestamp"),
                col("source"),
                col("event_time"),
                col("is_valid"),
                col("error_reason")
            )).alias("value")
        )
        dlq_kafka_payload.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", DLQ_TOPIC) \
            .save()

    # ---------------------------------------------------------------------
    # Step 5: Tumbling Window (1 Minute) Monitoring Aggregation
    # ---------------------------------------------------------------------
    print("\n" + "=" * 90)
    print(f"📊 [SPARK STREAMING MONITOR] Micro-Batch ID: {batch_id} | Time: {current_time_str}")
    print("=" * 90)

    # 1-Minute Tumbling Window for Valid Transactions
    if not valid_records.isEmpty() and "event_time" in valid_records.columns:
        valid_with_time = valid_records.filter(col("event_time").isNotNull())
        if not valid_with_time.isEmpty():
            window_df = valid_with_time.groupBy(
                window(col("event_time"), "1 minute")
            ).agg(
                count("*").alias("valid_count"),
                _sum("amount").alias("window_total_amount"),
                _avg("amount").alias("window_avg_amount")
            ).select(
                date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
                date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
                col("valid_count"),
                col("window_total_amount"),
                col("window_avg_amount")
            )
            print("\n📈 [TUMBLING WINDOW - 1 MINUTE MONITORING]")
            window_df.show(truncate=False)

    # ---------------------------------------------------------------------
    # Step 6: Console Output with Mandatory Columns: [timestamp, running_total]
    # ---------------------------------------------------------------------
    console_metrics = spark.createDataFrame([{
        "timestamp": current_time_str,
        "running_total": float(cumulative_state["running_total"]),
        "batch_id": int(batch_id),
        "batch_valid": int(batch_valid_count),
        "batch_dlq": int(batch_dlq_count),
        "cum_valid_count": int(cumulative_state["total_valid_count"]),
        "cum_dlq_count": int(cumulative_state["total_dlq_count"]),
    }])

    print("📌 [ASSIGNMENT REQUIRED CONSOLE OUTPUT: timestamp & running_total]")
    console_metrics.select(
        col("timestamp"),
        col("running_total"),
        col("batch_id"),
        col("batch_valid"),
        col("batch_dlq"),
        col("cum_valid_count"),
        col("cum_dlq_count")
    ).show(truncate=False)

    # Show preview of processed transactions with validation status
    print("🔍 [TRANSACTION VALIDATION & DLQ DETAILS]")
    processed_df.select(
        col("user_id"),
        col("amount"),
        col("timestamp"),
        col("source"),
        col("is_valid"),
        col("error_reason")
    ).show(truncate=False)

    batch_df.unpersist()


def main():
    print("=" * 80)
    print("⚡ SPARK STRUCTURED STREAMING JOB STARTING")
    print(f"Kafka Broker         : {KAFKA_BROKER}")
    print(f"Input Topic          : {INPUT_TOPIC}")
    print(f"Valid Output Topic   : {VALID_OUTPUT_TOPIC}")
    print(f"DLQ Output Topic     : {DLQ_TOPIC}")
    print(f"Checkpoint Dir       : {CHECKPOINT_DIR}")
    print("=" * 80)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read Stream from Kafka Topic
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Parse JSON & Extract Event Time with 3-minute Watermark
    schema = get_transaction_schema()

    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withWatermark("event_time", "3 minutes")

    # 3. Process Stream using foreachBatch (routes to Kafka valid & DLQ, updates running total, calculates windows)
    query = parsed_stream \
        .writeStream \
        .foreachBatch(evaluate_batch) \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    print("[SPARK STREAMING] Pipeline is active and awaiting data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
