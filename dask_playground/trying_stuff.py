# Databricks notebook source
import numpy as np
import pandas as pd

import dask.dataframe as dd
import dask.array as da
import dask.bag as db

from dask.distributed import Client

client = Client(n_workers=6, threads_per_worker=6)
client

# COMMAND ----------

index = pd.date_range("2021-09-01", periods=2400, freq="1H")
df = pd.DataFrame({"a": np.arange(2400), "b": list("abcaddbe" * 300)}, index=index)
ddf = dd.from_pandas(df, npartitions=10)

# COMMAND ----------

ddf["2021-10-01": "2021-10-09 5:00"].compute()

# COMMAND ----------

ddf.a.mean().compute()

# COMMAND ----------

ddf.b.unique()

# COMMAND ----------

ddf.b.unique().compute()

# COMMAND ----------

result = ddf["2021-10-01": "2021-10-09 5:00"].a.cumsum() - 100
result.compute()

# COMMAND ----------

result.visualize()

# COMMAND ----------

# MAGIC %md
# MAGIC ## more stuff

# COMMAND ----------

import dask

df = dask.datasets.timeseries(end="2001")

# COMMAND ----------

df.count().compute()

# COMMAND ----------


