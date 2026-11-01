# Databricks notebook source
import os
from functools import reduce
import json
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.types as T


# COMMAND ----------

spark.sql("USE workspace.macro_demo")
RAW = "/Volumes/workspace/macro_demo/raw"


# COMMAND ----------

frames = []
for f in dbutils.fs.ls(RAW):
    if not f.path.endswith(".csv"):
        continue
    series_id = os.path.basename(f.path).replace(".csv", "").upper()
    df = spark.read.option("header", True).csv(f.path)
    date_col, value_col = df.columns[0], df.columns[1]
    frames.append(df.select(                       # <- 4 spaces in, inside the loop
        F.lit(series_id).alias("series_id"),
        F.col(date_col).alias("obs_date_raw"),
        F.col(value_col).alias("value_raw"),
        F.lit(f.path).alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    ))


# COMMAND ----------

bronze = reduce(lambda a, b: a.unionByName(b), frames)

# COMMAND ----------

bronze.write.mode("append").saveAsTable("bronze_fred")

# COMMAND ----------

display(spark.sql("SELECT series_id, COUNT(*) AS n FROM bronze_fred GROUP BY series_id"))