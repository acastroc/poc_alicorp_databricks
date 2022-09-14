# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists gold.laundry_pc_hh ;
# MAGIC create table gold.laundry_pc_hh
# MAGIC (
# MAGIC ANO STRING,
# MAGIC MES STRING,
# MAGIC SEMANA STRING,
# MAGIC CATEGORIA  STRING,
# MAGIC COD_MATERIAL STRING,
# MAGIC DESCRIPCION_MATERIAL STRING,
# MAGIC CADENA STRING,
# MAGIC LOCAL STRING,
# MAGIC PVP DECIMAL(22,3),
# MAGIC DETALLE STRING,
# MAGIC TACTIC_ID STRING,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_bwtpm/gold/laundry_pc_hh'
# MAGIC     
