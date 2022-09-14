# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t024x ;
# MAGIC create table bronze.t024x
# MAGIC (
# MAGIC mandt string ,
# MAGIC spras string ,
# MAGIC labor string ,
# MAGIC lbtxt string ,
# MAGIC create_at timestamp,
# MAGIC origin_file string ,
# MAGIC year_month_day string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/t024x'
# MAGIC     
