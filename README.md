# joint_bdm
The BDM part of joint project

## Dataset Integration
The original dataset and codes to integrate dataset are in folder `/join_fact_table`.

## KPI
The code and the results for KPI analysis are in folder `/KPI`.

## Algo
The recommendation algorithm and sample output are in folder `/algo`.

## Stream Analysis
The stream analysis part are in folder `/stream_analysis`.
We use Kafka to stream the data. Kafka read data from csv and Spark read data from Kafka. Then Spark applied the trained algorithm on every batch. To run this part, make sure that Kafka is installed on the local environment.
For these command, they all run on Mac M1. Commands may change on different systems.

```bash
# open terminal 1, start zookeeper
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
# open terminal 2, start kafka server
kafka-server-start /opt/homebrew/etc/kafka/server.properties
# train the collaborative filtering model
python stream_analysis/rec_event_spark_train.py
# open terminal 3, Kafka read data from csv
python stream_analysis/rec_kafka_producer.py
# open terminal 4, Spark read data from Kafka
python stream_analysis/rec_kafka_producer.py
```

To check Kafka is working, you can also use

```bash
# open terminal 3, Kafka read data from csv
python stream_analysis/rec_kafka_producer.py
# open terminal 4, Spark read data from Kafka
stream_analysis/rec_kafka_consumer.py
```

If there is output, then Kafka works well.