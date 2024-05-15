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