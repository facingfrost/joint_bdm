# joint_bdm
The BDM part of joint project

## Environment Setup
```bash
conda create --name joint python=3.9
pip install -r requirements.txt
```

Delta Lake is downloaded as a python package. To check pyspark, type `pyspark` in terminal.

Note that java environment need to be configured.

Check local java environment by

```bash
java -version
echo $PATH
```


[Jenv](https://www.jenv.be/) is recommended to manage java environments.

## Data Preparation
[UCI online retail data](https://archive.ics.uci.edu/dataset/352/online+retail) is used to immitate user-event interaction. Implict feedback is adopted as users don't usually generate feedback for events.

In `preprocess/preprocess_online_retail.py`, original retail data is transformed into (user, product, quantity) tuple. Then it's saved into delta lake directly in a text format.

## Model Training

In `algo/rec_event_spark_train.py`, spark read text file directly from delta lake and transformed into (user, product, rating) tuple as matrix factorization model required. Then the model is saved to `target/tmp/myCollaborativeFilter`. However, currently MSE is very high, further validation is needed.

## Recommendation Prediction
In `algo/rec_event_spark_predict.py`, spark loaded the model trained and recommend 5 product to every user.

## How to run through the whole pipeline
```bash
# preprocess the data and store to delta lake, the downloaded raw data is input/Online Retail.xlsx and the csv file should be stored in input/online_retail_processed.csv
python preprocess/process_online_retail.py
# train the ALS recommendation model, the model will be stored in target/tmp/myCollaborativeFilter
python algo/rec_event_spark_train.py
# make recommendation for every user, the recommendation result will be stored in output_rec/output_retail.csv
python algo/rec_event_spark_predict.py
```

## Location-based recommendation
To implement this, we need to store user location and event location to database first.
We use the `latlng` in `user-data.csv` as user location and `latlng` in `1post.csv` as event location.

```bash
# store location csv into delta lake
python preprocess/process_location_based.py
# recommend 5 closest events for every user
python algo/rec_location_spark_train.py
```

Then location-based recommendation is stored in local files.

## Stream ingestion and prediction
We use Kafka to stream the data. Kafka read data from csv and Spark read data from Kafka. Then Spark applied the trained algorithm on every batch. To run this part, make sure that Kafka is installed on the local environment.
For these command, they all run on Mac M1. Commands may change on different systems.

```bash
# open terminal 1, start zookeeper
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
# open terminal 2, start kafka server
kafka-server-start /opt/homebrew/etc/kafka/server.properties
# open terminal 3, Kafka read data from csv
python algo/rec_kafka_producer.py
# open terminal 4, Spark read data from Kafka
python algo/rec_event_kafka_spark_predict.py
```

To check Kafka is working, you can also use

```bash
# open terminal 3, Kafka read data from csv
python algo/rec_kafka_producer.py
# open terminal 4, Spark read data from Kafka
python algo/kafka_consumer.py
```

If there is output, then Kafka works well.