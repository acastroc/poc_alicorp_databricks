# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t438t ;
# MAGIC create table bronze.t438t
# MAGIC (
# MAGIC mandt string ,
# MAGIC spras string ,
# MAGIC dismm string ,
# MAGIC dibez string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/t438t'
# MAGIC     
