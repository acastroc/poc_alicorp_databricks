# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t014t ;
# MAGIC create table bronze.t014t
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC KKBER string,
# MAGIC KKBTX string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH)
# MAGIC LOCATION '/mnt/data_s4/bronze/t014t'
# MAGIC     
