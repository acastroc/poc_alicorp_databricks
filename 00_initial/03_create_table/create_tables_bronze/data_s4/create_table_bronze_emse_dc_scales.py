# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.emse_dc_scales ;
# MAGIC create table bronze.emse_dc_scales
# MAGIC 
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC PRONR STRING,
# MAGIC SCALE STRING,
# MAGIC LOW STRING,
# MAGIC HIGH STRING,
# MAGIC 
# MAGIC CREATE_AT TIMESTAMP,
# MAGIC YEAR_MONTH_DAY STRING,
# MAGIC ORIGIN_FILE STRING
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/bronze/emse_dc_scales'
