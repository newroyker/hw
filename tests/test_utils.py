import pytest

import pandas as pd
from pyspark.sql import SparkSession
from lib.utils import xform, store, load, multiply_func

class TestUtils(object):
    spark = SparkSession.builder.master("local").config("spark.jars.packages", "io.delta:delta-core_2.12:0.1.0").config("spark.driver.host", "localhost").appName("hw_tests").getOrCreate()

    def test_multiply_func(self):
        ip = pd.Series([1, 2, 3])
        ex = pd.Series([1, 4, 9])

        op = multiply_func(ip, ip)

        assert(op.equals(ex))
        
    def test_xform(self):
    	ip = pd.Series([1, 2, 3])
    	ex = pd.Series([1, 4, 9])
    	ip_df = self.spark.createDataFrame(pd.DataFrame(ip, columns=["id"]))
    	
        op_df = xform(ip_df)
    	
        ex_df = self.spark.createDataFrame(pd.DataFrame(ex, columns=["id"]))
    	assert(ex_df.collect() == op_df.collect())

    def test_store(self):
        delta_path = "/tmp/roy/hw_store_delta_table"
    	ip_df = self.spark.range(10, 12)

    	store(ip_df, delta_path)

    	op_df = self.spark.read.format("delta").load(delta_path)
    	assert(op_df.collect() == ip_df.collect())

    def test_load(self):
        delta_path = "/tmp/roy/hw_load_delta_table"
        ip_df = self.spark.range(41, 43)
        ip_df.write.format("delta").mode("overwrite").save(delta_path)

        op_df = load(self.spark, delta_path)

        assert(op_df.collect() == ip_df.collect())
