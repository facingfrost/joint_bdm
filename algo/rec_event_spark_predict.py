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
import pandas as pd

# $example on$
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PythonCollaborativeFilteringExample")
    model_path = "target/tmp/myCollaborativeFilter"
    loaded_model = MatrixFactorizationModel.load(sc, model_path)
    # Generate top 10 movie recommendations for each user
    all_user_recommendations_rdd = loaded_model.recommendProductsForUsers(5)

    # Convert the RDD to a Python list of tuples
    all_user_recommendations = all_user_recommendations_rdd.collect()

    recommendation_list = []
    # Print recommendations for each user
    for user_recommendations in all_user_recommendations:
        user_id = user_recommendations[0]  # User ID
        recommendations = user_recommendations[1]  # List of recommendations
        for rec in recommendations:
            recommendation_list.append((user_id,rec.product,rec.rating))
    
    df = pd.DataFrame(recommendation_list, columns=['CustomerID', 'StockID', 'Rating'])
    df.to_csv("output_rec/output_retail.csv",index=False)