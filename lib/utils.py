from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

def multiply_func(a, b):
    return a * b

def xform(df):
	multiply = pandas_udf(multiply_func, returnType=LongType())
	return df.select(multiply(col("id"), col("id"))).withColumnRenamed("multiply_func(id, id)", "squared")

def store(df, delta_path):
	df.write.format("delta").mode("overwrite").save(delta_path)

def load(spark, delta_path):
	return spark.read.format("delta").load(delta_path)
	