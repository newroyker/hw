# Databricks notebook source
from lib.utils import xform, load, store
import pandas as pd

# COMMAND ----------

df = spark.range(1, 10)
display(df)

# COMMAND ----------

df_prime = xform(df)

# COMMAND ----------

display(
	df_prime
)

# COMMAND ----------

delta_path = "/tmp/roy/hw_dp"

# COMMAND ----------

store(df_prime, delta_path)

# COMMAND ----------

display(
	load(spark, delta_path)
)

# COMMAND ----------

