# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1: Read application data

# COMMAND ----------

APPLICATION_TABLE = "pricing_uk_production.engine_silver.dataset_validation"
APPLICATION_DATA_PATH = "s3://prima-uk-pricing-engine-pii-staging/bass_tests/application_data"

# COMMAND ----------

application = spark.read.table(APPLICATION_TABLE).limit(10000)
application.count()

# COMMAND ----------

application.write.mode('overwrite').parquet(APPLICATION_DATA_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC In theory, we could arrive to this point reading avro bytes

# COMMAND ----------

from dask import dataframe as dd

application = dd.read_parquet(APPLICATION_DATA_PATH)
application.head()

# COMMAND ----------

application_bag = application.to_bag(index=True, format="dict")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define feature extractors in python

# COMMAND ----------

from datetime import datetime

def acorn_group(dataset):
    return {"acorn.group": dataset["acorn"]["group"]}

def quote_hour(dataset):
    quote_timestamp = datetime.fromisoformat(dataset["meta"]["original_quote_timestamp"][:-8])
    return {"quote_hour": quote_timestamp.hour}

# COMMAND ----------

EXTRACTORS = {
    acorn_group,
    quote_hour
}


def process_live_application(data):
    pricing_dataset = data["dataset"]
    output = {}
    for extractor in EXTRACTORS:
        output.update(extractor(pricing_dataset))
    return output


def process_historical_application(data):
    features = process_live_application(data)
    features["risk_id"] = data["risk_id"]
    features["lineage_id"] = data["lineage_id"]
    return features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply extractors on data and inspect results

# COMMAND ----------

# MAGIC %md
# MAGIC Live data

# COMMAND ----------

live_application_example = application_bag.take(1)[0]
live_application_example

# COMMAND ----------

process_live_application(live_application_example)

# COMMAND ----------

# MAGIC %md
# MAGIC Historical data

# COMMAND ----------

historical_features = application_bag.map(process_historical_application).to_dataframe()

# COMMAND ----------

historical_features.head()

# COMMAND ----------

len(historical_features)

# COMMAND ----------

quote_hour_per_group = historical_features.groupby("acorn.group").agg({"quote_hour": "mean"}).compute()
quote_hour_per_group

# COMMAND ----------

type(quote_hour_per_group)

# COMMAND ----------


