# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvfkt ;
# MAGIC create table bronze.tvfkt
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC FKART string,
# MAGIC VTEXT string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvfkt'
# MAGIC     
