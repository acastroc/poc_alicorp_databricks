# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvmst ;
# MAGIC create table bronze.tvmst
# MAGIC (
# MAGIC mandt string ,
# MAGIC spras string ,
# MAGIC vmsta string ,
# MAGIC vmstb string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvmst'
# MAGIC     
