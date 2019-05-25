from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

def multiply_func(a, b):
    return a * b

def xform(df):
	multiply = pandas_udf(multiply_func, returnType=LongType())
	return df.select(multiply(col("x"), col("x"))).withColumnRenamed("multiply_func(x, x)", "squared")

def persist(df, table_path):
	df.write.format("parquet").mode("overwrite").save(table_path)
	