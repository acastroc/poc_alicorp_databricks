# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tcurt ;
# MAGIC create table bronze.tcurt
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC WAERS string,
# MAGIC LTEXT string,
# MAGIC KTEXT string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH)
# MAGIC LOCATION '/mnt/data_s4/bronze/tcurt'
# MAGIC     
