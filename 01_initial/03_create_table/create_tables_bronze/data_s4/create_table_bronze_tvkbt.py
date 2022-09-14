# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvkbt ;
# MAGIC create table bronze.tvkbt
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC VKBUR string,
# MAGIC BEZEI string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvkbt'
# MAGIC     
