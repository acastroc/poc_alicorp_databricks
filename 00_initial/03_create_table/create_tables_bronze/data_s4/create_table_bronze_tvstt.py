# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvstt ;
# MAGIC create table bronze.tvstt
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC VSTEL string,
# MAGIC VTEXT string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvstt'
# MAGIC     
