# process online_retail to spark input

import sys
import pandas as pd
import numpy as np
from scipy.stats import rankdata
import pyspark
from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *

retail_df = pd.read_excel('input/Online Retail.xlsx')
retail_df = retail_df[retail_df['CustomerID'].notna()&retail_df['StockCode'].notna()]
retail_df["StockCode"] = retail_df["StockCode"].astype(str)
retail_df["CustomerID"] = retail_df["CustomerID"].astype(int)

# count user buying times
grouped_df = retail_df[['CustomerID', 'StockCode', 'Description', 'Quantity']].groupby(['CustomerID', 'StockCode', 'Description']).sum().reset_index()
grouped_df.loc[grouped_df['Quantity'] == 0, ['Quantity']] = 1
grouped_df = grouped_df.loc[grouped_df['Quantity'] > 0]

# string code to int code
stocks = sorted(set(grouped_df["StockCode"]))
stocks_dict = dict(zip(stocks, range(1,len(stocks)+1)))

# output csv
grouped_df['StockID'] = grouped_df['StockCode'].map(stocks_dict)
selected_columns = grouped_df[["CustomerID","StockID","Quantity"]]
selected_columns.to_csv('input/online_retail_processed.csv', index=False, header=False)
print("Write csv success!")

# csv to delta lake
# Initialize SparkSession
#  Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("DeltaTutorial") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Write csv to Delta Lake
csv_rdd = spark.sparkContext.textFile("input/online_retail_processed.csv")
csv_rdd.saveAsTextFile("data/delta-table")