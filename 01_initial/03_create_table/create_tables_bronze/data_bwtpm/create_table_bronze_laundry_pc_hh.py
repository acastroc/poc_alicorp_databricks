# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists bronze.laundry_pc_hh ;
# MAGIC create table bronze.laundry_pc_hh
# MAGIC (
# MAGIC ANO STRING,
# MAGIC MES STRING,
# MAGIC SEMANA STRING,
# MAGIC CATEGORIA STRING,
# MAGIC COD_MATERIAL STRING,
# MAGIC DESCRIPCION_MATERIAL STRING,
# MAGIC CADENA STRING,
# MAGIC LOCAL STRING,
# MAGIC PVP STRING,
# MAGIC DETALLE STRING,
# MAGIC TACTIC_ID STRING,
# MAGIC 
# MAGIC CREATE_AT timestamp,
# MAGIC YEAR_MONTH_DAY string,
# MAGIC ORIGIN_FILE string
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (YEAR_MONTH_DAY)
# MAGIC LOCATION '/mnt/data_bwtpm/bronze/laundry_pc_hh'
# MAGIC     
