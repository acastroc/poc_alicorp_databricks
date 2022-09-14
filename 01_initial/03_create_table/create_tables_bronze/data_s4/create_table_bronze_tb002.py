# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tb002 ;
# MAGIC create table bronze.tb002
# MAGIC (
# MAGIC MANDT string ,
# MAGIC SPRAS string ,
# MAGIC BU_GROUP string ,
# MAGIC TXT15 string ,
# MAGIC TXT40 string ,
# MAGIC CREATE_AT timestamp ,
# MAGIC ORIGIN_FILE string ,
# MAGIC YEAR_MONTH_DAY string 
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/tb002'
# MAGIC     
