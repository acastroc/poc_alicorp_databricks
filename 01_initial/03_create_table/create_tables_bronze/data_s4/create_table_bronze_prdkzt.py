# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.prdkzt ;
# MAGIC create table bronze.prdkzt
# MAGIC (
# MAGIC mandt string ,
# MAGIC spras string ,
# MAGIC iprkz string ,
# MAGIC eprkz string ,
# MAGIC prtxt string ,
# MAGIC create_at timestamp,
# MAGIC origin_file string ,
# MAGIC year_month_day string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/prdkzt'
# MAGIC     
