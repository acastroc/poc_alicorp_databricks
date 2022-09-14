# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t005t ;
# MAGIC create table bronze.t005t
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC LAND1 string,
# MAGIC LANDX string,
# MAGIC NATIO string,
# MAGIC LANDX50 string,
# MAGIC NATIO50 string,
# MAGIC PRQ_SPREGT string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH)
# MAGIC LOCATION '/mnt/data_s4/bronze/t005t'
# MAGIC     
