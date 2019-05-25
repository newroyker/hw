import pytest

import pandas as pd
from pyspark.sql import SparkSession
from lib.udfs import xform, persist

class TestUDFs(object):
    spark = SparkSession.builder.master("local").config("spark.jars.packages", "io.delta:delta-core_2.12:0.1.0").config("spark.driver.host", "localhost").appName("hw_tests").getOrCreate()

    def test_xform(self):

    	print(self.spark.version)

    	ip = pd.Series([1, 2, 3])
    	ex = pd.Series([1, 4, 9])

    	ip_df = self.spark.createDataFrame(pd.DataFrame(ip, columns=["x"]))

    	op_df = xform(ip_df)

    	ex_df = self.spark.createDataFrame(pd.DataFrame(ex, columns=["x"]))

    	assert(ex_df.collect() == op_df.collect())

    def test_persist(self):

    	print(self.spark.version)

    	ip = pd.Series([10, 11, 12])

    	ip_df = self.spark.createDataFrame(pd.DataFrame(ip, columns=["x"]))

    	persist(ip_df, "/tmp/roy/hw_delta_table")

    	op_df = self.spark.read.format("delta").load("/tmp/roy/hw_delta_table")

    	ex_df = self.spark.createDataFrame(pd.DataFrame(ip, columns=["x"]))

    	assert(ex_df.collect() == op_df.collect())
