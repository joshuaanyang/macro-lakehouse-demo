# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark.sql("USE workspace.macro_demo")

# COMMAND ----------

clean=(spark.table("bronze_fred")
       .withColumn("obs_date", F.to_date("obs_date_raw"))
       .withColumn("value", F.expr("try_cast(value_raw as double)"))
       .filter("obs_date IS NOT NULL AND value IS NOT NULL")
       )

# COMMAND ----------

w=Window.partitionBy("series_id","obs_date").orderBy(F.col("ingested_at").desc())
updates=(clean.withColumn("rn", F.row_number().over(w)).filter("rn=1")
         .select("series_id","obs_date","value","ingested_at")
        )
updates.createOrReplaceTempView("silver_updates")


# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_fred
(series_id STRING, obs_date DATE, value DOUBLE, ingested_at TIMESTAMP)
""")
spark.sql("""
MERGE INTO silver_fred t
USING silver_updates s
ON t.series_id = s.series_id AND t.obs_date = s.obs_date
WHEN MATCHED AND t.value <> s.value THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

meta = spark.createDataFrame([
    ("DGS2",     "US 2-year Treasury yield",  "US", "rates",    "daily",   "percent",           "monetary policy", "FRED", 5),
    ("DGS10",    "US 10-year Treasury yield", "US", "rates",    "daily",   "percent",           "term premium",    "FRED", 5),
    ("DEXUSUK",  "USD per GBP spot rate",     "GB", "fx",       "daily",   "USD per GBP",       "fx",              "FRED", 12),
    ("CPIAUCSL", "US CPI all items, SA",      "US", "economic", "monthly", "index 1982-84=100", "inflation",       "FRED", 75),
], "series_id string, name string, country string, asset_class string, frequency string, "
   "unit string, theme string, source string, max_staleness_days int")
meta.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("series_metadata")
display(meta)

# COMMAND ----------

