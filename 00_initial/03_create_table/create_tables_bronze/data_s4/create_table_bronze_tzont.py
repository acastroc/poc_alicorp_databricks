# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tzont ;
# MAGIC create table bronze.tzont
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC LAND1 string,
# MAGIC ZONE1 string,
# MAGIC VTEXT string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH)
# MAGIC LOCATION '/mnt/data_s4/bronze/tzont'
# MAGIC     
