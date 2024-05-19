from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split
from pyspark.sql.types import DoubleType
from math import radians, cos, sin, asin, sqrt
from pyspark.sql import Row, SparkSession
from delta import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F

#  Create a spark session with Delta
builder = SparkSession.builder.appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# load user and post
user = spark.read.format("delta").load("data/user")
split_col = split(user["latlng"], ", ")
user = user.withColumn("u_lat", split_col.getItem(0).cast("float"))
user = user.withColumn("u_lon", split_col.getItem(1).cast("float"))

event = spark.read.format("delta").load("data/post")
split_col = split(event["latlng"], ", ")
event = event.withColumn("e_lat", split_col.getItem(0).cast("float"))
event = event.withColumn("e_lon", split_col.getItem(1).cast("float"))

# Define a UDF to calculate the Haversine distance
def haversine(lat1, lon1, lat2, lon2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6371 * c  # Radius of Earth in kilometers
    return km

haversine_udf = udf(haversine, DoubleType())

# Cross join users and events to calculate the distance
user_event_pairs = user.crossJoin(event)

# Add a new column with the calculated distance
user_event_pairs = user_event_pairs.withColumn("distance", haversine_udf(
    col("u_lat"), col("u_lon"),
    col("e_lat"), col("e_lon")
))

# Select relevant columns
user_event_pairs = user_event_pairs.select("user_id", "event_id", "distance")

window_spec = Window.partitionBy("user_id").orderBy("distance")

# Add a row number to each event based on the distance for each user
user_event_pairs = user_event_pairs.withColumn("rank", F.row_number().over(window_spec))

# Filter to get the top 5 closest events for each user
closest_events = user_event_pairs.filter(col("rank") <= 5).drop("rank")

# Show the result
# closest_events.show(truncate=False)
# Write the result
closest_events.write.csv("output_rec/location_based_rec.csv", header=True, mode="overwrite")