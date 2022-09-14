# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t134t ;
# MAGIC create table bronze.t134t
# MAGIC (
# MAGIC mandt string ,
# MAGIC spras string ,
# MAGIC mtart string ,
# MAGIC mtbez string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (year_month_day)
# MAGIC LOCATION '/mnt/data_s4/bronze/t134t'
# MAGIC     
