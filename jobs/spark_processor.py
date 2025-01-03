from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = 'financial_transactions'
AGGREGATES_TOPIC = 'transaction_aggregates'
ANOMALIES_TOPIC = 'transaction_anomalies'
CHECKPOINT_DIR = '/mnt/spark-checkpoints'
STATES_DIR = '/mnt/spark-state'

# Initialize Spark Session
spark = (SparkSession.builder
         .appName('FinancialTransactionsProcessor')
         .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
         .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
         .config('spark.sql.streaming.stateStore.stateStoreDir', STATES_DIR)
         .config('spark.sql.shuffle.partitions', 20)  # Fixed typo: .CONFIG to .config
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Define transaction schema
transaction_schema = StructType([
    StructField('transactionId', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('merchantId', StringType(), True),
    StructField('amount', DoubleType(), True),  # Use DoubleType for numeric calculations
    StructField('transactionTime', LongType(), True),  # Use LongType for epoch time
    StructField('transactionType', StringType(), True),
    StructField('location', StringType(), True),
    StructField('paymentMethod', StringType(), True),
    StructField('isInternational', BooleanType(), True),
    StructField('currency', StringType(), True),
])

# Read Kafka stream
kafka_stream = (spark.readStream
                .format("kafka")
                .option('kafka.bootstrap.servers', KAFKA_BROKERS)
                .option('subscribe', SOURCE_TOPIC)
                .option('startingOffsets', 'earliest')
                .option('failOnDataLoss', 'false') # Add this option
                .load())

# Parse JSON and extract fields
transactions_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col('value'), transaction_schema).alias("data")) \
    .select("data.*")

# Add a timestamp column for transaction time
transactions_df = transactions_df.withColumn(
    'transactionTimestamp',
    (col('transactionTime') / 1000).cast("timestamp")
)

# Aggregate transactions
aggregated_df = transactions_df.groupBy("merchantId") \
    .agg(
        sum("amount").alias('totalAmount'),
        count("*").alias("transactionCount")
    )

# Write aggregated data to Kafka
aggregation_query = (aggregated_df.withColumn("key", col('merchantId').cast("string"))
                     .withColumn("value", to_json(struct(
                         col("merchantId"),
                         col('totalAmount'),
                         col("transactionCount")
                     )))
                     .selectExpr("key", "value")
                     .writeStream
                     .format('kafka')
                     .outputMode('update')  # Use 'update' mode for streaming aggregation
                     .option('kafka.bootstrap.servers', KAFKA_BROKERS)
                     .option('topic', AGGREGATES_TOPIC)  # Fixed variable name
                     .option('checkpointLocation', f'{CHECKPOINT_DIR}/aggregates')
                     .start())

# Wait for termination
aggregation_query.awaitTermination()
