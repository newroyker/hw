import pytest

import pandas as pd
from pyspark.sql import SparkSession
from lib.udfs import xform

class TestMission(object):

    def test_with_life_goal(self):

        spark = SparkSession.builder.master("local").appName("hw_tests").getOrCreate()

        ip = pd.Series([1, 2, 3])
        ex = pd.Series([1, 4, 9])

        ip_df = df = spark.createDataFrame(pd.DataFrame(ip, columns=["x"]))

        op_df = xform(ip_df)

        ex_df = spark.createDataFrame(pd.DataFrame(ex, columns=["x"]))

        assert(ex_df.collect() == op_df.collect())
