# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t179 ;
# MAGIC create table bronze.t179
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC PRODH STRING,
# MAGIC STUFE STRING,
# MAGIC CREATE_AT TIMESTAMP,
# MAGIC YEAR_MONTH_DAY STRING,
# MAGIC ORIGIN_FILE STRING
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/t179'
# MAGIC     
