# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t006a ;
# MAGIC create table bronze.t006a
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC MSEHI string,
# MAGIC MSEH3 string,
# MAGIC MSEH6 string,
# MAGIC MSEHT string,
# MAGIC MSEHL string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/t006a'
# MAGIC     
