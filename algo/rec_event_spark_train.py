#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Collaborative Filtering Classification Example.
"""
from pyspark import SparkContext
import pyspark
from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *

# $example on$
from pyspark.ml.recommendation import ALS
# from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row, SparkSession
# $example off$

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("RatingsExample") \
        .getOrCreate()
    # $example on$
    # Load and parse the data
    # data = sc.textFile("input/online_retail_processed.csv")
    lines = spark.read.text("data/delta-table").rdd
    parts = lines.map(lambda row: row.value.split(","))
    ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), eventId=int(p[1]),
                                        rating=float(p[2])))
    ratings = spark.createDataFrame(ratingsRDD)
    (training, test) = ratings.randomSplit([0.9, 0.1])
    # Build the recommendation model using Alternating Least Squares
    rank = 10
    numIterations = 10
    # For explicit feedback
    # model = ALS.train(ratings, rank, numIterations)
    # For implicit feedback
    # model = ALS.trainImplicit(training, rank, numIterations)
    als = ALS(maxIter=5, regParam=0.01, implicitPrefs=True, userCol="userId", itemCol="eventId", ratingCol="rating",
            coldStartStrategy="drop")
    model = als.fit(training)
    print("Train model success!")

    # Evaluate the model on training data
    predictions_train = model.transform(training)
    evaluator_train = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator_train.evaluate(predictions_train)
    print("Root-mean-square error of training set = " + str(rmse))

    # Evaluate the model on test data
    predictions_test = model.transform(test)
    evaluator_test = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator_test.evaluate(predictions_test)
    print("Root-mean-square error of test set = " + str(rmse))

    # Save and load model
    # model.save(spark, "target/tmp/myCollaborativeFilter")
    model.write().overwrite().save("target/tmp/myCollaborativeFilter")
    # sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
    # $example off$