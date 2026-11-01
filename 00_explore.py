# Databricks notebook source
print(dbutils.fs.head("/Volumes/workspace/macro_demo/raw/DGS10.csv", 500))

# COMMAND ----------

df = spark.read.option("header", True).csv("/Volumes/workspace/macro_demo/raw/DGS10.csv")
display(df.limit(10))     # first 10 rows as a table
df.printSchema()          # column names and types (all "string" for now)
print(df.count())         # how many rows

# COMMAND ----------

from pyspark.sql import functions as F
value_col = df.columns[1]

display(df.agg(
    F.min(df.columns[0]).alias("first_date"),
    F.max(df.columns[0]).alias("last_date"),
    F.count("*").alias("rows"),
    F.sum(F.when(F.col(value_col).isNull() | (F.col(value_col) == "."), 1).otherwise(0)).alias("missing"),
))

# values that won't convert to a number
display(df.filter(F.expr(f"try_cast({value_col} AS DOUBLE) IS NULL")).limit(10))

# COMMAND ----------

for name in ["DGS2", "DGS10", "DEXUSUK", "CPIAUCSL"]:
    d = spark.read.option("header", True).csv(f"/Volumes/workspace/macro_demo/raw/{name}.csv")
    print(name, d.columns, d.count())

# COMMAND ----------

