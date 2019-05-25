# Databricks notebook source
from lib.udfs import xform
import pandas as pd

# COMMAND ----------

x = pd.Series([1, 2, 3])

# COMMAND ----------

df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

# COMMAND ----------

display(df)

# COMMAND ----------

display(xform(df))

# COMMAND ----------

