# Databricks notebook source
from lib.udfs import xform, persist
import pandas as pd

# COMMAND ----------

x = pd.Series([4, 5, 6])
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))
display(df)

# COMMAND ----------

df_prime = xform(df)

# COMMAND ----------

display(df_prime)

# COMMAND ----------

persist(df_prime, "/tmp/roy/hw_delta_table")

# COMMAND ----------

display(spark.read.format("delta").load("/tmp/roy/hw_delta_table"))

# COMMAND ----------

