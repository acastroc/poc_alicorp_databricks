# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvast ;
# MAGIC create table bronze.tvast
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC AUFSP string,
# MAGIC VTEXT string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvast'
# MAGIC     
