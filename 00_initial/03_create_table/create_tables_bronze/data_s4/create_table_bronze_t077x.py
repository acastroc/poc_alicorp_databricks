# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t077x ;
# MAGIC create table bronze.t077x
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC KTOKD string,
# MAGIC TXT30 string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH)
# MAGIC LOCATION '/mnt/data_s4/bronze/t077x'
# MAGIC     
