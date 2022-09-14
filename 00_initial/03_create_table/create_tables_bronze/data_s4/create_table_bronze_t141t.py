# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.t141t ;
# MAGIC create table bronze.t141t
# MAGIC (
# MAGIC MANDT string ,
# MAGIC SPRAS string ,
# MAGIC MMSTA string ,
# MAGIC MTSTB string ,
# MAGIC CREATE_AT timestamp ,
# MAGIC ORIGIN_FILE string ,
# MAGIC YEAR_MONTH_DAY string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/t141t'
# MAGIC     
