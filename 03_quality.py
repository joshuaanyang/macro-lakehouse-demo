# Databricks notebook source
from pyspark.sql import functions as F
spark.sql("USE workspace.macro_demo")

# COMMAND ----------

s=spark.table("silver_fred")
m=spark.table("series_metadata")
checks=[]

# COMMAND ----------

dupes = s.groupBy("series_id", "obs_date").count().filter("count > 1").count()
checks.append(("There are no duplicate keys", dupes == 0,dupes ))


# COMMAND ----------

no_meta=s.join(m, on="series_id", how="left_anti").select("series_id").distinct().count()
checks.append(("all_series_have_meta", no_meta == 0, no_meta))

# COMMAND ----------

bad_rates = s.filter("series_id IN ('DGS2','DGS10') AND (value < -5 OR value > 25)").count()
checks.append(("rates_in_range", bad_rates == 0, bad_rates))

# COMMAND ----------

stale = (s.groupBy("series_id")
  .agg(F.max("obs_date").alias("last_date"))
  .join(m,"series_id")
  .filter("(frequency = 'daily' AND datediff(current_date(), last_date) > 12) OR" "(frequency = 'monthly' AND datediff(current_date(), last_date) > 75) ")
  .count())
checks.append(("data_is_fresh", stale == 0, stale))

# COMMAND ----------

results = (spark.createDataFrame([(n,bool(p), int(v)) for n, p, v in checks],
                                "check string, passed boolean, failing long")
           .withColumn("run_at",F.current_timestamp()))
results.write.mode("append").saveAsTable("data_quality_results")
display(results)

# COMMAND ----------

failed = [n for n, p, v in checks if not p]
if failed:
  raise Exception(f"Quality gate failed: {failed}")

# COMMAND ----------

display(s.groupBy("series_id")
         .agg(F.max("obs_date").alias("last_date"))
         .withColumn("days_old", F.datediff(F.current_date(), "last_date")))

# COMMAND ----------

