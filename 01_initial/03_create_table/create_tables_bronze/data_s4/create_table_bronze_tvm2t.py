# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvm2t ;
# MAGIC create table bronze.tvm2t
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC MVGR2 string,
# MAGIC BEZEI string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvm2t'
# MAGIC     
