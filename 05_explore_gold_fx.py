# Databricks notebook source
spark.sql("USE workspace.macro_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT obs_date, spread_2s10s_bp, gbpusd_vol_20d, cpi_ref_month, cpi_yoy
# MAGIC FROM gold_rates_fx_daily 
# MAGIC ORDER BY obs_date 
# MAGIC DESC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_fred;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_fred VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE gold_rates_fx_daily IS
# MAGIC 'Daily US rates and GBPUSD features with point-in-time US CPI. Source: FRED via silver_fred.'

# COMMAND ----------

