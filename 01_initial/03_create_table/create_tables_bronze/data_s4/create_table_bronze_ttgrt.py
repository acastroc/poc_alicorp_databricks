# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.ttgrt ;
# MAGIC create table bronze.ttgrt
# MAGIC (
# MAGIC mandt string ,
# MAGIC spras string ,
# MAGIC tragr string ,
# MAGIC vtext string ,
# MAGIC create_at timestamp ,
# MAGIC origin_file string ,
# MAGIC year_month_day string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/ttgrt'
# MAGIC     
