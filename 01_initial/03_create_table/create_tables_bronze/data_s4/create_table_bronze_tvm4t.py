# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvm4t ;
# MAGIC create table bronze.tvm4t
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC MVGR4 string,
# MAGIC BEZEI string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvm4t'
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from bronze.tvm4t
