# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.tvkmt ;
# MAGIC create table bronze.tvkmt
# MAGIC (
# MAGIC MANDT string,
# MAGIC SPRAS string,
# MAGIC KTGRM string,
# MAGIC VTEXT string,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/tvkmt'
# MAGIC     
