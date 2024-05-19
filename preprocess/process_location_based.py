import pyspark
from delta import *
import pandas as pd

#  Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Write csv to Delta Lake
post = spark.read.csv("processed_csv/1post.csv", header=True, inferSchema=True)
post.write.format("delta").save("data/post")

# post = pd.read_csv("processed_csv/1post.csv")
# post.to_csv('input/post.csv', index=False, header=False)
# post_rdd = spark.sparkContext.textFile("input/post.csv")
# post_rdd.saveAsTextFile("data/post")

user = spark.read.csv("processed_csv/user-data.csv", header=True, inferSchema=True)
user.write.format("delta").save("data/user")

# user = pd.read_csv("processed_csv/user-data.csv")
# user.to_csv('input/user.csv', index=False, header=False)
# user_rdd = spark.sparkContext.textFile("input/user.csv")
# user_rdd.saveAsTextFile("data/user")