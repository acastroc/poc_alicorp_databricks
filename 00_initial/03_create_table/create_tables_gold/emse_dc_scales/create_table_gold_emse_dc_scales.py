# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists gold.emse_dc_scales ;
# MAGIC create table gold.emse_dc_scales
# MAGIC 
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC PRONR STRING,
# MAGIC SCALE STRING,
# MAGIC LOW DECIMAL(22,3),
# MAGIC HIGH DECIMAL(22,3),
# MAGIC 
# MAGIC CREATE_AT TIMESTAMP,
# MAGIC YEAR_MONTH_DAY STRING,
# MAGIC ORIGIN_FILE STRING
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/gold/emse_dc_scales'
