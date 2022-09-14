# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.makt ;
# MAGIC create table bronze.makt
# MAGIC (
# MAGIC MANDT string,
# MAGIC MATNR string,
# MAGIC SPRAS string,
# MAGIC MAKTX string,
# MAGIC MAKTG string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/makt'
# MAGIC     
