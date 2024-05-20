from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.ml.recommendation import ALSModel
import findspark
import os

findspark.init()
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "10") \
    .getOrCreate()


# Define Kafka source
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "user-events"

# Define schema for incoming data
schema = StructType([
    StructField("userId", StringType()),
    StructField("eventId", StringType()),
    StructField("rating", StringType())  # Adjust the fields as per your CSV structure
])

# Read stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()


# Convert the Kafka value column to a string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON string into structured columns
# parsed_df = kafka_df.select(from_json(col("value"), schema))
parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data"))
df = parsed_df.select("data.*")
df = df.withColumn("userId", col("userId").cast("int"))
df = df.withColumn("eventId", col("eventId").cast("int"))
df = df.withColumn("rating", col("rating").cast("float"))

# Write the processed data to the console (for debugging purposes)
# query = kafka_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# Wait for the termination signal
# query.awaitTermination()


# Write the processed data to the console (for debugging purposes)
# query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# Wait for the termination signal
# query.awaitTermination()

# # Load pre-trained ALS model
model_path = "target/tmp/myCollaborativeFilter"
als_model = ALSModel.load(model_path)

# # Function to make predictions
def make_predictions(batch_df, batch_id):
    batch_df = batch_df.na.drop()
    predictions = als_model.transform(batch_df)
    # Define the output file path
    output_path = os.path.join(os.getcwd(), f"output_rec/stream_applied_res/predictions_batch_{batch_id}.csv")
    
    # Save predictions to CSV
    predictions.select("userId", "eventId", "prediction").toPandas().to_csv(output_path, index=False)
    predictions.show()

# # Apply the prediction function on each micro-batch
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(make_predictions) \
    .start()

query.awaitTermination()
