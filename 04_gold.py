# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
spark.sql("USE workspace.macro_demo")

# COMMAND ----------

silver = spark.table("silver_fred").filter("obs_date >= '2000-01-01'")


# COMMAND ----------

daily = (silver.filter("series_id IN ('DGS2', 'DGS10','DEXUSUK') ")
         .groupBy("obs_date").pivot("series_id",["DGS2","DGS10","DEXUSUK"]).agg(F.first("value"))
         )

# COMMAND ----------

w = Window.orderBy("obs_date")

# COMMAND ----------


daily = (daily
         .withColumn("spread_2s10s_bp", (F.col("DGS10")-F.col("DGS2"))*100)
         .withColumn("gbpusd_ret", F.col("DEXUSUK")/F.lag("DEXUSUK",1).over(w)-1)
         .withColumn("gbpusd_vol_20d", F.stddev("gbpusd_ret").over(w.rowsBetween(-19,0))*F.sqrt(F.lit(252))))


# COMMAND ----------

cpi = (spark.table("silver_fred").filter("series_id = 'CPIAUCSL'")
       .withColumn("cpi_yoy", F.col("value")/F.lag("value",12).over(Window.orderBy("obs_date"))-1)
       .withColumn("available_date", F.date_add(F.add_months("obs_date", 1), 14))
       .select(F.col("obs_date").alias("cpi_ref_month"), "available_date","cpi_yoy"))

# COMMAND ----------

joined = daily.alias("d").join(cpi.alias("c"), F.col("c.available_date") <= F.col("d.obs_date"), "left")

# COMMAND ----------

pick = Window.partitionBy("d.obs_date").orderBy(F.col("c.available_date").desc()) 

# COMMAND ----------

gold = (joined
        .withColumn("rn", F.row_number().over(pick))
        .filter("rn = 1")
        .select("d.*","c.cpi_ref_month","c.cpi_yoy")
       )

# COMMAND ----------

gold.write.mode("overwrite").saveAsTable("gold_rates_fx_daily")

# COMMAND ----------

display(gold,tail=10)

# COMMAND ----------

