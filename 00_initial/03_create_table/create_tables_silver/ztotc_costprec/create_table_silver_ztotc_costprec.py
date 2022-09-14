# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists silver.ztotc_costprec ;
# MAGIC create table silver.ztotc_costprec
# MAGIC 
# MAGIC (
# MAGIC MANDT STRING,
# MAGIC MES STRING,
# MAGIC SEMANA STRING,
# MAGIC VKORG STRING,
# MAGIC OF_R3 STRING,
# MAGIC CANAL_R3 STRING,
# MAGIC SKU INT,
# MAGIC PREC_PLAN DECIMAL(22,3),
# MAGIC PREC_REAL DECIMAL(22,3),
# MAGIC COST_PLAN DECIMAL(22,3),
# MAGIC COST_REAL DECIMAL(22,3),
# MAGIC 
# MAGIC CREATE_AT TIMESTAMP,
# MAGIC YEAR_MONTH_DAY STRING,
# MAGIC ORIGIN_FILE STRING
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_s4/silver/ztotc_costprec'