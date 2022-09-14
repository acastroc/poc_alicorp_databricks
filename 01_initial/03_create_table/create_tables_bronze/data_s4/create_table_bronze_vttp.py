# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.vttp ;
# MAGIC create table bronze.vttp
# MAGIC (
# MAGIC mandt string,
# MAGIC tknum string,
# MAGIC tpnum string,
# MAGIC vbeln string,
# MAGIC tprfo string,
# MAGIC ernam string,
# MAGIC erdat string,
# MAGIC erzet string,
# MAGIC pksta string,
# MAGIC kzhulfg string,
# MAGIC create_at timestamp,
# MAGIC year_month_day string,
# MAGIC origin_file string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (year_month_day)
# MAGIC LOCATION '/mnt/data_s4/bronze/vttp'
# MAGIC     
